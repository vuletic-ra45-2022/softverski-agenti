{-# LANGUAGE StrictData #-}

module Control.Actor.Core
  ( ActorM (..)
  , runActorM
  , state
  , getSelf
  , Actor
  , ActorResult (..)
  , Handler
  , liftRuntime
  , notifyOfDeath
  , spawnActor
  , spawnActorAs
  , linkActorTo
  , linkTo
  , killActor
  , stopOnDeath
  , cast'
  , call'
  , cast
  , call
  , castIn
  , pass
  , passWith
  , continue
  , maybeContinue
  , become
  , lastMessageFrom
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
import Data.Maybe (fromMaybe)
import Control.Monad.Reader
  ( MonadIO (..)
  , MonadReader (..)
  , ReaderT (..)
  , asks
  , withReaderT
  )
import Data.Binary (Binary, decode, encode)
import Data.Map qualified as Map
import Data.UUID (UUID)
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

lastMessageFrom :: ActorM u NodeId
lastMessageFrom = asks (asLatestFrom . fst)

getSelf :: ActorM u SomeActorRef
getSelf = do
  (as, rt) <- ask
  actors <- liftIO $ readTVarIO (rtActors rt)
  let actorId  = asId as
      maybeRef = snd <$> Map.lookup actorId actors
  case maybeRef of
    Just ref -> return ref
    Nothing  -> error "getSelf: actor not found in runtime"

data ActorResult msg u r = ActorResult
  { actorReply  :: Maybe r
  , actorState  :: u
  , actorBecome :: Maybe (msg -> ActorM u (ActorResult msg u r))
  }

type Actor msg u r = ActorM u (ActorResult msg u r)

type Handler msg u r = msg -> Actor msg u r

liftRuntime :: RuntimeM a -> ActorM u a
liftRuntime = ActorM . withReaderT snd . unRuntimeM

notifyOfDeath :: Runtime -> DeathMessage -> DeathTarget -> IO ()
notifyOfDeath _  dm (LocalTarget q)         = atomically $ writeTQueue q dm
notifyOfDeath rt dm (RemoteTarget _ nodeId) =
  withRuntime rt $ do
    maybeAddr <- lookupNode nodeId
    case maybeAddr of
      Nothing   -> return ()
      Just addr -> rtSendRemote rt addr (NMDeath (dmActorId dm) (toRemoteExitReason (dmReason dm)))

spawnActorAs ::
  (Binary m, Binary r) =>
  UUID ->
  Handler m u r ->
  (DeathMessage -> ActorM u (SupervisorAction u)) ->
  u ->
  RuntimeM (ActorRef m r)
spawnActorAs uuid actorFn deathFn initState = do
  rt      <- ask
  mailbox <- liftIO newTQueueIO
  deathQ  <- liftIO newTQueueIO
  links   <- liftIO $ newTVarIO []
  let actorId    = ActorId thisNodeId uuid
      actorState = ActorState actorId links initState thisNodeId
      actorRef   = LocalRef mailbox deathQ actorState

  let loop fn as = do
        event <-
          atomically $
            (Left <$> readTQueue mailbox)
              `orElse` (Right <$> readTQueue deathQ)
        case event of
          Left envelope ->
            case envelope of
              (from, Cast msg) -> do
                ActorResult _ u' mFn <- runActorM (fn msg) rt as {asLatestFrom = from}
                loop (fromMaybe fn mFn) as {asEnv = u'}
              (from, Call msg mv) -> do
                ActorResult reply u' mFn <- runActorM (fn msg) rt as {asLatestFrom = from}
                putMVar mv reply
                loop (fromMaybe fn mFn) as {asEnv = u'}
          Right dm -> do
            action <- runActorM (deathFn dm) rt as
            case action of
              Stop       -> return ()
              Continue u -> loop fn as {asEnv = u}

  tid <- liftIO $ forkIO $ do
    result <- try @SomeException (loop actorFn actorState)
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

spawnActor ::
  (Binary m, Binary r) =>
  Handler m u r ->
  (DeathMessage -> ActorM u (SupervisorAction u)) ->
  u ->
  RuntimeM (ActorRef m r)
spawnActor actorFn deathFn initState = do
  uuid <- liftIO nextRandom
  spawnActorAs uuid actorFn deathFn initState

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
  liftIO $ atomically $ writeTQueue arMsgQ (thisNodeId, Cast msg)
cast' msg (RemoteRef (ActorId nodeId uuid)) = do
  rt <- ask
  maybeAddr <- lookupNode nodeId
  case maybeAddr of
    Nothing   -> liftIO $ putStrLn $ "cast: no node in lookup table with id " <> show nodeId
    Just addr -> rtSendRemote rt addr (NMCast uuid (encode msg))

call' :: (Binary msg, Binary reply) => msg -> ActorRef msg reply -> RuntimeM (Maybe reply)
call' msg (LocalRef {arMsgQ}) = liftIO $ do
  mv <- newEmptyMVar
  atomically $ writeTQueue arMsgQ (thisNodeId, Call msg mv)
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

continue :: r -> Actor msg u r
continue x = (\u -> ActorResult (Just x) u Nothing) <$> state

maybeContinue :: Maybe r -> Actor msg u r
maybeContinue Nothing = pass
maybeContinue (Just r) = continue r

pass :: Actor msg u r
pass = (\u -> ActorResult Nothing u Nothing) <$> state

passWith :: u -> Actor msg u r
passWith u = return (ActorResult Nothing u Nothing)

become :: Handler msg u r -> Actor msg u r
become fn = (\u -> ActorResult Nothing u (Just fn)) <$> state
