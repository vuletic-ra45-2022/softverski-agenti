{-# LANGUAGE StrictData #-}

module Control.Actor.Core
  ( ActorM (..)
  , runActorM
  , state
  , getSelf
  , Actor
  , liftRuntime
  , notifyOfDeath
  , spawnActor
  , linkActorTo
  , linkTo
  , killActor
  , stopOnDeath
  , cast'
  , call'
  , cast
  , call
  , castIn
  ) where

import Control.Actor.Runtime
import Control.Actor.Types
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM
  ( atomically
  , modifyTVar
  , newTQueueIO
  , newTVarIO
  , orElse
  , readTQueue
  , readTVar
  , readTVarIO
  , writeTQueue
  , writeTVar
  )
import Control.Exception
  ( AsyncException (..)
  , SomeException
  , fromException
  , throwIO
  , try
  )
import Control.Monad (forM_, void)
import Control.Monad.Reader
  ( MonadIO (..)
  , MonadReader (..)
  , ReaderT (..)
  , asks
  , withReaderT
  )
import Data.Binary (Binary, decode, encode)
import Data.Map qualified as Map
import Data.UUID.V4 (nextRandom)

newtype ActorM u r = ActorM
  { unActorM :: ReaderT (ActorState u, Runtime) IO r }
  deriving
    ( Functor, Applicative, Monad, MonadIO
    , MonadReader (ActorState u, Runtime)
    )

runActorM :: ActorM u r -> Runtime -> ActorState u -> IO r
runActorM m rt s = runReaderT (unActorM m) (s, rt)

state :: ActorM u u
state = asks (asEnv . fst)

getSelf :: ActorM u SomeActorRef
getSelf = do
  (as, rt) <- ask
  actors <- liftIO $ readTVarIO (rtActors rt)
  let actorId  = asId as
      maybeRef = snd <$> Map.lookup actorId actors
  case maybeRef of
    Just ref -> return ref
    Nothing  -> error "getSelf: actor not found in runtime"

type Actor u r = ActorM u (Maybe r, u)

liftRuntime :: RuntimeM a -> ActorM u a
liftRuntime = ActorM . withReaderT snd . unRuntimeM

notifyOfDeath :: Runtime -> DeathMessage -> DeathTarget -> IO ()
notifyOfDeath _  dm (LocalTarget q)           = atomically $ writeTQueue q dm
notifyOfDeath rt dm (RemoteTarget _ peerAddr) =
  withRuntime rt $ rtSendRemote rt peerAddr (NMDeath (dmActorId dm) (toRemoteExitReason (dmReason dm)))

spawnActor ::
  (Binary m, Binary r) =>
  (m -> Actor u r) ->
  (DeathMessage -> ActorM u (SupervisorAction u)) ->
  u ->
  RuntimeM (ActorRef m r)
spawnActor actorFn deathFn initState = do
  rt      <- ask
  mailbox <- liftIO newTQueueIO
  deathQ  <- liftIO newTQueueIO
  links   <- liftIO $ newTVarIO []
  uuid    <- liftIO nextRandom
  let actorId    = ActorId thisNodeId uuid
      actorState = ActorState actorId links initState
      actorRef   = LocalRef mailbox deathQ actorState

  let loop as = do
        event <-
          atomically $
            (Left <$> readTQueue mailbox)
              `orElse` (Right <$> readTQueue deathQ)
        case event of
          Left envelope ->
            case envelope of
              Cast msg -> do
                (_, u') <- runActorM (actorFn msg) rt as
                loop as {asEnv = u'}
              Call msg mv -> do
                (reply, u') <- runActorM (actorFn msg) rt as
                putMVar mv reply
                loop as {asEnv = u'}
          Right dm -> do
            action <- runActorM (deathFn dm) rt as
            case action of
              Stop       -> return ()
              Continue u -> loop as {asEnv = u}

  tid <- liftIO $ forkIO $ do
    result <- try @SomeException (loop actorState)
    let reason = case result of
          Right () -> Normal
          Left exc -> case fromException exc of
            Just ThreadKilled -> Killed
            _anyOtherExc      -> Exception exc
    links' <- readTVarIO (asLinks actorState)
    forM_ links' (notifyOfDeath rt (DeathMessage actorId reason))
    atomically $ modifyTVar (rtActors rt) (Map.delete actorId)
    case result of
      Left exc -> throwIO exc
      Right () -> return ()

  liftIO $ atomically $
    modifyTVar (rtActors rt) (Map.insert actorId (tid, SomeActorRef actorRef))
  return actorRef

linkActorTo :: DeathTarget -> ActorRef m r -> RuntimeM ()
linkActorTo target (LocalRef {arState}) =
  liftIO $ atomically $ modifyTVar (asLinks arState) (target :)
linkActorTo _ (RemoteRef _) = return ()

linkTo :: DeathTarget -> ActorM u ()
linkTo target = do
  as <- asks fst
  liftIO $ atomically $ modifyTVar (asLinks as) (target :)

killActor :: ActorRef m r -> ActorM u ()
killActor (LocalRef {arDeathQ, arState}) =
  liftIO $ atomically $ writeTQueue arDeathQ (DeathMessage (asId arState) Killed)
killActor (RemoteRef _) =
  liftIO $ putStrLn "killActor: remote kill not yet implemented"

stopOnDeath :: DeathMessage -> ActorM u (SupervisorAction u)
stopOnDeath _ = return Stop

cast' :: (Binary msg) => msg -> ActorRef msg reply -> RuntimeM ()
cast' msg (LocalRef {arMsgQ}) =
  liftIO $ atomically $ writeTQueue arMsgQ (Cast msg)
cast' msg (RemoteRef (ActorId nodeId uuid)) = do
  rt <- ask
  maybeAddr <- lookupNode nodeId
  case maybeAddr of
    Nothing   -> liftIO $ putStrLn $ "cast: no node in lookup table with id " <> show nodeId
    Just addr -> rtSendRemote rt addr (NMCast uuid (encode msg))

call' :: (Binary msg, Binary reply) => msg -> ActorRef msg reply -> RuntimeM (Maybe reply)
call' msg (LocalRef {arMsgQ}) = liftIO $ do
  mv <- newEmptyMVar
  atomically $ writeTQueue arMsgQ (Call msg mv)
  takeMVar mv
call' msg (RemoteRef (ActorId nodeId uuid)) = do
  rt <- ask
  corrId <- liftIO $ atomically $ do
    cid <- readTVar (rtNextCorr rt)
    writeTVar (rtNextCorr rt) (cid + 1)
    return cid
  replyVar <- liftIO newEmptyMVar
  liftIO $ atomically $ modifyTVar (rtPending rt) (Map.insert corrId replyVar)
  maybeAddr <- lookupNode nodeId
  case maybeAddr of
    Nothing -> liftIO $ do
      putStrLn $ "call: no node in lookup table with id " <> show nodeId
      atomically $ modifyTVar (rtPending rt) (Map.delete corrId)
      return Nothing
    Just addr -> do
      rtSendRemote rt addr (NMCall uuid corrId (rtNodeId rt) (encode msg))
      raw <- liftIO $ takeMVar replyVar
      liftIO $ atomically $ modifyTVar (rtPending rt) (Map.delete corrId)
      return $ Just (decode raw)

cast :: (Binary msg) => msg -> ActorRef msg reply -> ActorM u ()
cast = (liftRuntime .) . cast'

castIn :: (Binary msg) => Int -> msg -> ActorRef msg reply -> ActorM u ()
castIn ms msg ref = do
  rt <- asks snd
  liftIO $ void $ forkIO $ do
    threadDelay (ms * 1000)
    withRuntime rt $ cast' msg ref

call :: (Binary msg, Binary reply) => msg -> ActorRef msg reply -> ActorM u (Maybe reply)
call = (liftRuntime .) . call'
