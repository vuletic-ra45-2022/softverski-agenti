{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE StrictData #-}

module Control.Actor.Supervision
  ( ChildSpec (..)
  , child
  , childWithRef
  , RestartStrategy (..)
  , ChildSlot (..)
  , spawnSlot
  , supervise'
  , supervise
  , doOneForOne
  , doOneForAll
  , doRestForOne
  , killSlot
  ) where

import Control.Actor.Core (ActorM, Handler, liftRuntime, linkActorTo, spawnActor)
import Control.Actor.Runtime (Runtime (..), RuntimeM, withRuntime)
import Control.Actor.Types
import Data.Binary (Binary)
import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Concurrent.STM
  ( TMVar
  , TQueue
  , TVar
  , atomically
  , flushTQueue
  , modifyTVar
  , newTQueueIO
  , newTVarIO
  , putTMVar
  , readTQueue
  , readTVarIO
  , tryTakeTMVar
  , writeTVar
  )
import Control.Monad (forever, void)
import Control.Monad.Reader (MonadIO (..), MonadReader (..))
import Data.Map qualified as Map

data ChildSpec = forall m r. (Binary m, Binary r) => ChildSpec
  { csRun     :: DeathTarget -> RuntimeM (ActorRef m r)
  , csOnSpawn :: ActorRef m r -> IO ()
  }

child ::
  (Binary m, Binary r) =>
  Handler m u r ->
  (DeathMessage -> ActorM u (SupervisorAction u)) ->
  u ->
  ChildSpec
child msgFn deathFn initState =
  ChildSpec
    { csRun = \target -> do
        ref <- spawnActor msgFn deathFn initState
        linkActorTo target ref
        return ref
    , csOnSpawn = \_ -> return ()
    }

childWithRef ::
  (Binary m, Binary r) =>
  Handler m u r ->
  (DeathMessage -> ActorM u (SupervisorAction u)) ->
  u ->
  TMVar (ActorRef m r) ->
  ChildSpec
childWithRef msgFn deathFn initState cell =
  ChildSpec
    { csRun = \target -> do
        ref <- spawnActor msgFn deathFn initState
        linkActorTo target ref
        return ref
    , csOnSpawn = \ref -> atomically $ do
        void $ tryTakeTMVar cell
        putTMVar cell ref
    }

data RestartStrategy = OneForOne | OneForAll | RestForOne

data ChildSlot = forall m r. (Binary m, Binary r) => ChildSlot
  { slotSpec :: ChildSpec
  , slotRef  :: ActorRef m r
  , slotId   :: ActorId
  , slotTid  :: ThreadId
  }

spawnSlot :: DeathTarget -> ChildSpec -> RuntimeM ChildSlot
spawnSlot target spec@ChildSpec {csRun, csOnSpawn} = do
  rt  <- ask
  ref <- csRun target
  _   <- liftIO $ csOnSpawn ref
  let aid = someActorId (SomeActorRef ref)
  tid <- liftIO $ do
    actors <- readTVarIO (rtActors rt)
    case Map.lookup aid actors of
      Just (t, _) -> return t
      Nothing     -> error "spawnSlot: actor vanished immediately after spawn"
  return $ ChildSlot spec ref aid tid

killSlot :: ChildSlot -> IO ()
killSlot ChildSlot {slotTid} = killThread slotTid

supervise' :: RestartStrategy -> [ChildSpec] -> RuntimeM ()
supervise' strategy specs = do
  rt        <- ask
  supDeathQ <- liftIO newTQueueIO
  let target = LocalTarget supDeathQ
  slots    <- mapM (spawnSlot target) specs
  slotsVar <- liftIO $ newTVarIO slots
  liftIO $ void $ forkIO $ forever $ do
    DeathMessage deadId _ <- atomically $ readTQueue supDeathQ
    slots' <- readTVarIO slotsVar
    withRuntime rt $ case strategy of
      OneForOne  -> doOneForOne target slotsVar slots' deadId
      OneForAll  -> doOneForAll target slotsVar supDeathQ slots'
      RestForOne -> doRestForOne target slotsVar supDeathQ slots' deadId

supervise :: RestartStrategy -> [ChildSpec] -> ActorM u ()
supervise = (liftRuntime .) . supervise'

doOneForOne ::
  DeathTarget -> TVar [ChildSlot] -> [ChildSlot] -> ActorId -> RuntimeM ()
doOneForOne target slotsVar slots deadId =
  case filter (\s -> slotId s == deadId) slots of
    []     -> return ()
    slot:_ -> do
      newSlot <- spawnSlot target (slotSpec slot)
      liftIO $ atomically $
        modifyTVar slotsVar $
          map (\s -> if slotId s == deadId then newSlot else s)

doOneForAll ::
  DeathTarget ->
  TVar [ChildSlot] ->
  TQueue DeathMessage ->
  [ChildSlot] ->
  RuntimeM ()
doOneForAll target slotsVar supDeathQ slots = do
  liftIO $ do
    mapM_ killSlot slots
    atomically $ void $ flushTQueue supDeathQ
  newSlots <- mapM (spawnSlot target . slotSpec) slots
  liftIO $ atomically $ writeTVar slotsVar newSlots

doRestForOne ::
  DeathTarget ->
  TVar [ChildSlot] ->
  TQueue DeathMessage ->
  [ChildSlot] ->
  ActorId ->
  RuntimeM ()
doRestForOne target slotsVar supDeathQ slots deadId = do
  let (before, fromDead) = break (\s -> slotId s == deadId) slots
  case fromDead of
    []        -> return ()
    _nonempty -> do
      liftIO $ do
        mapM_ killSlot (drop 1 fromDead)
        atomically $ void $ flushTQueue supDeathQ
      newSlots <- mapM (spawnSlot target . slotSpec) fromDead
      liftIO $ atomically $ writeTVar slotsVar (before ++ newSlots)
