{-# LANGUAGE StrictData #-}

module Control.Actor where

import Control.Concurrent (ThreadId, forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM
  ( TQueue,
    TVar,
    atomically,
    modifyTVar,
    newTQueueIO,
    newTVar,
    newTVarIO,
    readTQueue,
    readTVar,
    writeTQueue,
    writeTVar,
  )
import Control.Monad.Reader (MonadIO (liftIO), MonadReader (ask), ReaderT (runReaderT), asks, lift, withReaderT)
import Data.Binary (Binary, decode, encode)
import Data.ByteString.Lazy (ByteString)
import Data.Map qualified as Map
import Data.Set qualified as Set
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import GHC.Generics (Generic)

data NodeAddr = NodeAddr
  { nodeHost :: String,
    nodePort :: Integer
  }
  deriving (Eq, Show, Generic)

instance Binary NodeAddr

newtype ActorId = ActorId UUID
  deriving (Eq, Ord, Show, Generic)

instance Binary ActorId

data ActorState u = ActorState
  { asId :: ActorId,
    asLinks :: TVar (Set.Set SomeActorRef),
    asEnv :: u
  }

newtype ActorM u r = ActorM
  {unActorM :: ReaderT (ActorState u, Runtime) IO r}
  deriving
    ( Functor,
      Applicative,
      Monad,
      MonadIO,
      MonadReader (ActorState u, Runtime)
    )

runActorM :: ActorM u r -> Runtime -> ActorState u -> IO r
runActorM m r s = runReaderT (unActorM m) (s, r)

state :: ActorM u u
state = asks (asEnv . fst)

data ActorRef msg reply
  = forall u. LocalRef
      { arMsgQ :: TQueue (Envelope msg reply),
        arDeathQ :: TQueue DeathMessage,
        arState :: ActorState u
      }
  | RemoteRef ActorId NodeAddr

data SomeActorRef = forall msg reply. SomeActorRef (ActorRef msg reply)

data DeathMessage = DeathMessage

type CorrelationId = Integer

data Envelope msg reply
  = Cast msg
  | Call msg (MVar (Maybe reply))

data Runtime = Runtime
  { rtNodeId :: NodeAddr,
    rtActors :: TVar (Map.Map ActorId (ThreadId, SomeActorRef)), -- local registry
    rtPending :: TVar (Map.Map CorrelationId (MVar ByteString)), -- in-flight calls
    rtNextCorr :: TVar CorrelationId, -- correlation id counter
    rtTransport :: Transport -- transport capability
  }

data Transport = Transport
  { sendBytes :: NodeAddr -> ByteString -> IO ()
  }

data RemoteEnvelope
  = RemoteCast ActorId ByteString
  | RemoteCall ActorId CorrelationId NodeAddr ByteString
  | RemoteReply CorrelationId ByteString
  deriving (Generic)

instance Binary RemoteEnvelope

newtype RuntimeM a = RuntimeM
  {unRuntimeM :: ReaderT Runtime IO a}
  deriving
    ( Functor,
      Applicative,
      Monad,
      MonadIO,
      MonadReader Runtime
    )

liftRuntime :: RuntimeM a -> ActorM u a
liftRuntime = ActorM . withReaderT snd . unRuntimeM

withRuntime :: Runtime -> RuntimeM a -> IO a
withRuntime = (. unRuntimeM) . flip runReaderT
{-# INLINE withRuntime #-}

newRuntime :: IO Runtime
newRuntime = atomically $ do
  actors <- newTVar Map.empty
  pending <- newTVar Map.empty
  nextCorr <- newTVar (0 :: Integer)
  return
    Runtime
      { rtNodeId = NodeAddr "localhost" 0, -- TODO: replace with actual node addr
        rtActors = actors,
        rtPending = pending,
        rtNextCorr = nextCorr,
        rtTransport = Transport (\_a _b -> return ())
      }

cast' :: (Binary msg) => msg -> ActorRef msg reply -> RuntimeM ()
cast' msg (LocalRef {arMsgQ}) = RuntimeM $ lift $ atomically $ writeTQueue arMsgQ (Cast msg)
cast' msg (RemoteRef actorId addr) = do
  rt <- ask
  let msgBs = encode msg
      payload = encode (RemoteCast actorId msgBs)
  liftIO $ sendBytes (rtTransport rt) addr payload

cast :: (Binary msg) => msg -> ActorRef msg reply -> ActorM u ()
cast = (liftRuntime .) . cast'

call' :: (Binary msg, Binary reply) => msg -> ActorRef msg reply -> RuntimeM (Maybe reply)
call' msg (LocalRef {arMsgQ}) = liftIO $ do
  mv <- newEmptyMVar
  atomically $ writeTQueue arMsgQ (Call msg mv)
  takeMVar mv
call' msg (RemoteRef actorId addr) = do
  rt <- ask
  -- allocate a correlation id
  corrId <- liftIO $ atomically $ do
    cid <- readTVar (rtNextCorr rt)
    writeTVar (rtNextCorr rt) (cid + 1)
    return cid
  replyVar <- liftIO newEmptyMVar
  liftIO $ atomically $ modifyTVar (rtPending rt) (Map.insert corrId replyVar)

  -- send message over the network
  let payload = encode (RemoteCall actorId corrId (rtNodeId rt) (encode msg))
  liftIO $ sendBytes (rtTransport rt) addr payload

  -- block until we get a reply
  raw <- liftIO $ takeMVar replyVar
  -- clean up and decode
  liftIO $ atomically $ modifyTVar (rtPending rt) (Map.delete corrId)
  return $ decode raw

call :: (Binary msg, Binary reply) => msg -> ActorRef msg reply -> ActorM u (Maybe reply)
call = (liftRuntime .) . call'

spawnActor :: (m -> ActorM u (Maybe r, u)) -> u -> RuntimeM (ActorRef m r)
spawnActor actorFn initState = do
  rt <- ask
  mailbox <- liftIO newTQueueIO
  deathQ <- liftIO newTQueueIO
  links <- liftIO $ newTVarIO Set.empty

  uuid <- liftIO nextRandom
  let actorId = ActorId uuid
      actorState = ActorState actorId links initState
      actorRef = LocalRef mailbox deathQ actorState

  let loop as = do
        envelope <- atomically $ readTQueue mailbox
        case envelope of
          Cast msg -> do
            (_, u') <- runActorM (actorFn msg) rt as
            loop as { asEnv = u' }
          (Call msg mv) -> do
            (reply, u') <- runActorM (actorFn msg) rt as
            putMVar mv reply
            loop as { asEnv = u' }

  tid <- liftIO $ forkIO (loop actorState)
  liftIO $ atomically $ modifyTVar (rtActors rt) (Map.insert actorId (tid, SomeActorRef actorRef))
  return actorRef

type Actor u r = ActorM u (Maybe r, u)

----------------------------

pingActor :: String -> Actor () String
pingActor msg = return (Just ("Hello, " <> msg <> "!"), ())

forwardActor :: ActorRef String String -> String -> Actor () String
forwardActor otherRef msg = do
  reply <- call msg otherRef
  case reply of
    Nothing -> do
      liftIO $ putStrLn "forwardActor: received empty reply!"
    Just _ -> do
      liftIO $ putStrLn "forwardActor: received reply!"
  return (reply, ())

system :: IO ()
system = do
  rt <- newRuntime
  _ <- withRuntime rt $ do
    pingRef <- spawnActor pingActor ()
    spawnActor (forwardActor pingRef) ()

  return ()
