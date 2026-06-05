{-# LANGUAGE StrictData #-}

module Control.Actor where

import Control.Concurrent (ThreadId, forkIO, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM
  ( TMVar,
    TQueue,
    TVar,
    atomically,
    flushTQueue,
    modifyTVar,
    newEmptyTMVarIO,
    newTQueueIO,
    newTVar,
    newTVarIO,
    orElse,
    putTMVar,
    readTMVar,
    readTQueue,
    readTVar,
    readTVarIO,
    tryTakeTMVar,
    writeTQueue,
    writeTVar,
  )
import Control.Exception (AsyncException (..), SomeException, fromException, throwIO, try)
import Control.Monad (forM_, forever, void)
import Control.Monad.Reader (MonadIO (liftIO), MonadReader (ask), ReaderT (runReaderT), asks, lift, withReaderT)
import Data.Binary (Binary, decode, encode)
import Data.ByteString.Lazy (ByteString)
import Data.List (find)
import Data.Map qualified as Map
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import GHC.Generics (Generic)
import Unsafe.Coerce (unsafeCoerce)

data NodeAddr = NodeAddr
  { nodeHost :: String,
    nodePort :: Integer
  }
  deriving (Eq, Show, Generic)

instance Binary NodeAddr

type NodeId = Integer

thisNodeId :: NodeId
thisNodeId = 0

data ActorId = ActorId NodeId UUID
  deriving (Eq, Ord, Show, Generic)

instance Binary ActorId

data ExitReason
  = Normal
  | Killed
  | Exception SomeException
  deriving (Show, Generic)

data DeathMessage = DeathMessage
  { dmActorId :: ActorId,
    dmReason :: ExitReason
  }
  deriving (Show, Generic)

data DeathTarget
  = LocalTarget (TQueue DeathMessage)
  | RemoteTarget ActorId

data ActorState u = ActorState
  { asId :: ActorId,
    asLinks :: TVar [DeathTarget],
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

getSelf :: ActorM u SomeActorRef
getSelf = do
  (as, rt) <- ask
  actors <- liftIO $ readTVarIO (rtActors rt)
  let actorId = asId as
      maybeRef = snd <$> Map.lookup actorId actors
  case maybeRef of
    Just ref -> return ref
    Nothing  -> error "getSelf: actor not found in runtime"

data ActorRef msg reply
  = forall u. LocalRef
      { arMsgQ :: TQueue (Envelope msg reply),
        arDeathQ :: TQueue DeathMessage,
        arState :: ActorState u
      }
  | RemoteRef ActorId

data SomeActorRef = forall msg reply. SomeActorRef (ActorRef msg reply)

someActorId :: SomeActorRef -> ActorId
someActorId (SomeActorRef (LocalRef {arState})) = asId arState
someActorId (SomeActorRef (RemoteRef aid)) = aid

data SupervisorAction u
  = Stop
  | Continue u

type CorrelationId = Integer

data Envelope msg reply
  = Cast msg
  | Call msg (MVar (Maybe reply))

data Runtime = Runtime
  { rtNodeId :: NodeAddr,
    rtActors :: TVar (Map.Map ActorId (ThreadId, SomeActorRef)),
    rtPending :: TVar (Map.Map CorrelationId (MVar ByteString)),
    rtNextCorr :: TVar CorrelationId,
    rtNodeTable :: TVar (Map.Map NodeId NodeAddr),
    rtTransport :: Transport
  }

data Transport = Transport
  { sendBytes :: NodeAddr -> ByteString -> IO ()
  }

data RemoteEnvelope
  = RemoteCast UUID ByteString
  | RemoteCall UUID CorrelationId NodeAddr ByteString
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
  nodeTable <- newTVar Map.empty
  return
    Runtime
      { rtNodeId = NodeAddr "localhost" 0,
        rtActors = actors,
        rtPending = pending,
        rtNextCorr = nextCorr,
        rtNodeTable = nodeTable,
        rtTransport = Transport (\_ _ -> return ())
      }

lookupNode :: NodeId -> RuntimeM (Maybe NodeAddr)
lookupNode nodeId = do
  rt <- ask
  liftIO $ atomically $ do
    table <- readTVar $ rtNodeTable rt
    return $ Map.lookup nodeId table

cast' :: (Binary msg) => msg -> ActorRef msg reply -> RuntimeM ()
cast' msg (LocalRef {arMsgQ}) = RuntimeM $ lift $ atomically $ writeTQueue arMsgQ (Cast msg)
cast' msg (RemoteRef (ActorId nodeId uuid)) = do
  rt <- ask
  maybeAddr <- lookupNode nodeId
  let payload = encode (RemoteCast uuid (encode msg))
  case maybeAddr of
    Just addr -> liftIO $ sendBytes (rtTransport rt) addr payload
    Nothing -> liftIO $ putStrLn $ "cast: no node in lookup table with id " <> show nodeId

cast :: (Binary msg) => msg -> ActorRef msg reply -> ActorM u ()
cast = (liftRuntime .) . cast'

castIn :: (Binary msg) => Int -> msg -> ActorRef msg reply -> ActorM u ()
castIn ms msg ref = do
  rt <- asks snd
  liftIO $ void $ forkIO $ do
    threadDelay (ms * 1000)
    withRuntime rt $ cast' msg ref

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
  let payload = encode (RemoteCall uuid corrId (rtNodeId rt) (encode msg))
  maybeAddr <- lookupNode nodeId
  case maybeAddr of
    Just addr -> do
      liftIO $ sendBytes (rtTransport rt) addr payload
      raw <- liftIO $ takeMVar replyVar
      liftIO $ atomically $ modifyTVar (rtPending rt) (Map.delete corrId)
      return $ decode raw
    Nothing -> do
      liftIO $ putStrLn $ "call: no node in lookup table with id " <> show nodeId
      return Nothing

call :: (Binary msg, Binary reply) => msg -> ActorRef msg reply -> ActorM u (Maybe reply)
call = (liftRuntime .) . call'

type Actor u r = ActorM u (Maybe r, u)

notifyOfDeath :: DeathMessage -> DeathTarget -> IO ()
notifyOfDeath dm (LocalTarget q) = atomically $ writeTQueue q dm
notifyOfDeath _ (RemoteTarget _) = return ()

spawnActor ::
  (m -> Actor u r) ->
  (DeathMessage -> ActorM u (SupervisorAction u)) ->
  u ->
  RuntimeM (ActorRef m r)
spawnActor actorFn deathFn initState = do
  rt <- ask
  mailbox <- liftIO newTQueueIO
  deathQ <- liftIO newTQueueIO
  links <- liftIO $ newTVarIO []
  uuid <- liftIO nextRandom
  let actorId = ActorId thisNodeId uuid
      actorState = ActorState actorId links initState
      actorRef = LocalRef mailbox deathQ actorState

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
              Stop -> return ()
              Continue u -> loop as {asEnv = u}

  tid <- liftIO $ forkIO $ do
    result <- try (loop actorState) :: IO (Either SomeException ())
    let reason = case result of
          Right () -> Normal
          Left exc -> case fromException exc of
            Just ThreadKilled -> Killed
            _anyOtherExc      -> Exception exc
    links' <- readTVarIO (asLinks actorState)
    let dm = DeathMessage actorId reason
    forM_ links' (notifyOfDeath dm)
    atomically $ modifyTVar (rtActors rt) (Map.delete actorId)
    case result of
      Left exc -> throwIO exc
      Right () -> return ()

  liftIO $ atomically $ modifyTVar (rtActors rt) (Map.insert actorId (tid, SomeActorRef actorRef))
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

-- Supervision

data ChildSpec = forall m r. ChildSpec
  { csRun :: DeathTarget -> RuntimeM (ActorRef m r),
    csOnSpawn :: ActorRef m r -> IO ()
  }

child ::
  (m -> Actor u r) ->
  (DeathMessage -> ActorM u (SupervisorAction u)) ->
  u ->
  ChildSpec
child msgFn deathFn initState =
  ChildSpec
    { csRun = \target -> do
        ref <- spawnActor msgFn deathFn initState
        linkActorTo target ref
        return ref,
      csOnSpawn = \_ -> return ()
    }

childWithRef ::
  (m -> Actor u r) ->
  (DeathMessage -> ActorM u (SupervisorAction u)) ->
  u ->
  TMVar (ActorRef m r) ->
  ChildSpec
childWithRef msgFn deathFn initState cell =
  ChildSpec
    { csRun = \target -> do
        ref <- spawnActor msgFn deathFn initState
        linkActorTo target ref
        return ref,
      csOnSpawn = \ref -> atomically $ do
        void $ tryTakeTMVar cell
        putTMVar cell ref
    }

data RestartStrategy = OneForOne | OneForAll | RestForOne

data ChildSlot = forall m r. ChildSlot
  { slotSpec :: ChildSpec,
    slotRef :: ActorRef m r,
    slotId :: ActorId
  }

spawnSlot :: DeathTarget -> ChildSpec -> RuntimeM ChildSlot
spawnSlot target spec@ChildSpec{csRun, csOnSpawn} = do
  ref <- csRun target
  _   <- liftIO $ csOnSpawn ref
  return $ ChildSlot spec ref (someActorId (SomeActorRef ref))

supervise' :: RestartStrategy -> [ChildSpec] -> RuntimeM ()
supervise' strategy specs = do
  rt <- ask
  supDeathQ <- liftIO newTQueueIO
  let target = LocalTarget supDeathQ
  slots <- mapM (spawnSlot target) specs
  slotsVar <- liftIO $ newTVarIO slots
  _ <- liftIO $ forkIO $ forever $ do
    DeathMessage deadId _ <- atomically $ readTQueue supDeathQ
    slots' <- readTVarIO slotsVar
    case strategy of
      OneForOne -> doOneForOne rt target slotsVar slots' deadId
      OneForAll -> doOneForAll rt target slotsVar supDeathQ slots'
      RestForOne -> doRestForOne rt target slotsVar supDeathQ slots' deadId
  return ()

supervise :: RestartStrategy -> [ChildSpec] -> ActorM u ()
supervise = (liftRuntime .) . supervise'

doOneForOne ::
  Runtime -> DeathTarget -> TVar [ChildSlot] -> [ChildSlot] -> ActorId -> IO ()
doOneForOne rt target slotsVar slots deadId =
  case find (\s -> slotId s == deadId) slots of
    Nothing -> return ()
    Just slot -> do
      newSlot <- withRuntime rt $ spawnSlot target (slotSpec slot)
      atomically $
        modifyTVar slotsVar $
          map (\s -> if slotId s == deadId then newSlot else s)

doOneForAll ::
  Runtime ->
  DeathTarget ->
  TVar [ChildSlot] ->
  TQueue DeathMessage ->
  [ChildSlot] ->
  IO ()
doOneForAll rt target slotsVar supDeathQ slots = do
  mapM_ (killSlot supDeathQ) slots
  atomically $ void $ flushTQueue supDeathQ
  newSlots <- withRuntime rt $ mapM (spawnSlot target . slotSpec) slots
  atomically $ writeTVar slotsVar newSlots

doRestForOne ::
  Runtime ->
  DeathTarget ->
  TVar [ChildSlot] ->
  TQueue DeathMessage ->
  [ChildSlot] ->
  ActorId ->
  IO ()
doRestForOne rt target slotsVar supDeathQ slots deadId = do
  let (before, fromDead) = break (\s -> slotId s == deadId) slots
  case fromDead of
    []        -> return ()
    _nonempty -> do
      mapM_ (killSlot supDeathQ) (drop 1 fromDead)
      atomically $ void $ flushTQueue supDeathQ
      newSlots <- withRuntime rt $ mapM (spawnSlot target . slotSpec) fromDead
      atomically $ writeTVar slotsVar (before ++ newSlots)

killSlot :: TQueue DeathMessage -> ChildSlot -> IO ()
killSlot _ (ChildSlot {slotRef, slotId}) = case slotRef of
  LocalRef {arDeathQ} -> atomically $ writeTQueue arDeathQ (DeathMessage slotId Killed)
  RemoteRef _ -> return ()

----- Demo

pingActor :: String -> Actor () String
pingActor msg = return (Just ("Hello, " <> msg <> "!"), ())

forwardActorWithCell :: TMVar (ActorRef String String) -> String -> Actor () String
forwardActorWithCell cell msg = do
  pingRef <- liftIO $ atomically $ readTMVar cell
  reply <- call msg pingRef
  case reply of
    Nothing -> liftIO $ putStrLn "forwardActorWithCell: received empty reply!"
    Just x -> liftIO $ putStrLn ("forwardActorWithCell: received reply - " <> x)
  return (reply, ())

repeatActor :: String -> TMVar (ActorRef String String) -> () -> Actor () String
repeatActor r cell () = do
  ref <- liftIO $ atomically $ readTMVar cell
  (SomeActorRef self) <- getSelf
  cast r ref
  castIn 1000 () (unsafeCoerce self)
  return (Nothing, ())

system :: IO ()
system = do
  rt <- newRuntime
  withRuntime rt $ do
    pingCell <- liftIO newEmptyTMVarIO
    forwardCell <- liftIO newEmptyTMVarIO
    repeatCell <- liftIO newEmptyTMVarIO

    _ <- liftIO $ forkIO $ do
      repeatRef <- atomically $ readTMVar repeatCell
      withRuntime rt $ do
        cast' () repeatRef

    supervise'
      OneForOne
      [ childWithRef pingActor stopOnDeath () pingCell,
        childWithRef (forwardActorWithCell pingCell) stopOnDeath () forwardCell,
        childWithRef (repeatActor "repeaaat" forwardCell) stopOnDeath () repeatCell
      ]
