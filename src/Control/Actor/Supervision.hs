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
  , TVar
  , atomically
  , modifyTVar
  , newTQueueIO
  , newTVarIO
  , putTMVar
  , readTQueue
  , readTVarIO
  , tryTakeTMVar
  , writeTVar
  )
import Control.Monad (void)
import GHC.Clock (getMonotonicTime)
import Control.Monad.Reader (MonadIO (..), MonadReader (..))
import Data.Map qualified as Map

data ChildSpec = forall m r. (Binary m, Binary r) => ChildSpec
  { csRun     :: DeathTarget -> RuntimeM (ActorRef m r)
  -- | Runs after every (re)spawn — including supervisor restarts — so it can
  -- refresh shared ref cells and re-register names.
  , csOnSpawn :: ActorRef m r -> RuntimeM ()
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
    , csOnSpawn = \ref -> liftIO $ atomically $ do
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
  csOnSpawn ref
  let aid = someActorId (SomeActorRef ref)
  tid <- liftIO $ do
    actors <- readTVarIO (rtActors rt)
    case Map.lookup aid actors of
      Just (t, _) -> return t
      Nothing     -> error "spawnSlot: actor vanished immediately after spawn"
  return $ ChildSlot spec ref aid tid

killSlot :: ChildSlot -> IO ()
killSlot ChildSlot {slotTid} = killThread slotTid

-- | Max child restarts within 'restartWindow' seconds before the supervisor
-- gives up and stops all children — otherwise a child that crashes right
-- after spawning would respawn in a hot loop forever.
restartLimit :: Int
restartLimit = 5

restartWindow :: Double
restartWindow = 10

supervise' :: RestartStrategy -> [ChildSpec] -> RuntimeM ()
supervise' strategy specs = do
  rt        <- ask
  supDeathQ <- liftIO newTQueueIO
  let target = LocalTarget supDeathQ
  slots    <- mapM (spawnSlot target) specs
  slotsVar <- liftIO $ newTVarIO slots
  let loop restarts = do
        DeathMessage deadId reason <- atomically $ readTQueue supDeathQ
        slots' <- readTVarIO slotsVar
        if all ((/= deadId) . slotId) slots'
          -- Stale death: killSlot is asynchronous, so children replaced by a
          -- OneForAll/RestForOne restart report their deaths after the fact.
          -- Acting on one would kill the fresh children and cascade forever.
          then loop restarts
          else case reason of
            -- A normal exit is completion, not a crash: drop the slot.
            Normal -> do
              atomically $ modifyTVar slotsVar (filter ((/= deadId) . slotId))
              loop restarts
            _crash -> do
              now <- getMonotonicTime
              let recent = filter (> now - restartWindow) restarts
              if length recent >= restartLimit
                then do
                  putStrLn $ "supervisor: " <> show restartLimit
                    <> " restarts within " <> show restartWindow
                    <> "s, giving up"
                  mapM_ killSlot slots'
                else do
                  withRuntime rt $ case strategy of
                    OneForOne  -> doOneForOne target slotsVar slots' deadId
                    OneForAll  -> doOneForAll target slotsVar slots'
                    RestForOne -> doRestForOne target slotsVar slots' deadId
                  loop (now : recent)
  liftIO $ void $ forkIO $ loop []

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
  [ChildSlot] ->
  RuntimeM ()
doOneForAll target slotsVar slots = do
  liftIO $ mapM_ killSlot slots
  newSlots <- mapM (spawnSlot target . slotSpec) slots
  liftIO $ atomically $ writeTVar slotsVar newSlots

doRestForOne ::
  DeathTarget ->
  TVar [ChildSlot] ->
  [ChildSlot] ->
  ActorId ->
  RuntimeM ()
doRestForOne target slotsVar slots deadId = do
  let (before, fromDead) = break (\s -> slotId s == deadId) slots
  case fromDead of
    []        -> return ()
    _nonempty -> do
      liftIO $ mapM_ killSlot (drop 1 fromDead)
      newSlots <- mapM (spawnSlot target . slotSpec) fromDead
      liftIO $ atomically $ writeTVar slotsVar (before ++ newSlots)
