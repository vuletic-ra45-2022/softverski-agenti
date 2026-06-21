{-# LANGUAGE RecordWildCards #-}
module FL.P2P
  ( runPeer
  ) where

import Control.Actor
import Control.Actor.Registry (registerActor, lookupRemoteActor)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar)
import Control.Concurrent.STM
  ( TMVar, atomically, newEmptyTMVarIO, putTMVar, readTMVar, tryTakeTMVar )
import Control.Monad (forM, forM_, void)
import Control.Monad.IO.Class (liftIO)
import FL.Actors.Logger (logFn, logFn', loggerHandler)
import FL.Actors.PeerCoordinator
import FL.CRDT (gcEmpty, orEmpty)
import FL.Data.PaySim (loadAndPartition)
import FL.NN
  ( LocalData, evaluate, flattenWeights, initWeights
  , localDataSize, trainLocal, unflattenWeights
  )
import FL.Retry (retryConnect, retryLookup)
import FL.Types

-- ---------------------------------------------------------------------------
-- P2P-specific actor state types
-- ---------------------------------------------------------------------------

-- Evaluator reads the PeerCoordinator ref lazily from a TMVar.
-- This breaks the circular dependency: evaluator is spawned before PC,
-- but PC's ref is filled into the TMVar right after PC spawns.
data P2PEvalState = P2PEvalState
  { p2eTestData :: LocalData
  , p2ePcCell   :: TMVar (ActorRef PeerCoordinatorMsg ())
  , p2eLogRef   :: ActorRef LogMsg ()
  }

p2pEvalHandler :: Handler EvaluatorMsg P2PEvalState ()
p2pEvalHandler (EvaluateModel gm) = do
  st <- state
  let ws      = unflattenWeights (wData (gmWeights gm))
      metrics = evaluate ws (p2eTestData st) (gmRound gm)
  logFn (p2eLogRef st) "Evaluator" INFO $
    "P2P round " <> show (gmRound gm)
    <> " acc=" <> show4 (mAccuracy metrics)
    <> " auc=" <> show4 (mAUCROC  metrics)
  pcRef <- liftIO $ atomically $ readTMVar (p2ePcCell st)
  cast (PeerEvaluationDone metrics) pcRef
  pass
  where
    show4 x = show (fromIntegral (round (x * 10000) :: Int) / 10000.0 :: Double)

-- Trainer likewise reads the PeerCoordinator ref lazily.
data P2PTrainerState = P2PTrainerState
  { p2tId        :: String
  , p2tData      :: LocalData
  , p2tLastRound :: Int
  , p2tPcCell    :: TMVar (ActorRef PeerCoordinatorMsg ())
  , p2tLogRef    :: ActorRef LogMsg ()
  , p2tConfig    :: FLConfig
  }

p2pTrainerHandler :: Handler TrainerMsg P2PTrainerState ()
p2pTrainerHandler (StartTraining gm) = do
  st <- state
  let round_ = gmRound gm
  if round_ <= p2tLastRound st
    then pass
    else do
      logFn (p2tLogRef st) (p2tId st) INFO $
        "P2P training round " <> show round_
      let initW = unflattenWeights (wData (gmWeights gm))
      updatedW <- liftIO $ trainLocal (p2tConfig st) (p2tId st) round_ initW (p2tData st)
      let n   = localDataSize (p2tData st)
          upd = ModelUpdate (p2tId st) round_ n (Weights (flattenWeights updatedW))
      pcRef <- liftIO $ atomically $ readTMVar (p2tPcCell st)
      cast (PeerTrainingDone upd) pcRef
      passWith st { p2tLastRound = round_ }
p2pTrainerHandler TrainerStop = pass

-- ---------------------------------------------------------------------------
-- Peer node startup
-- ---------------------------------------------------------------------------

runPeer :: FLConfig -> Int -> [NodeAddr] -> IO ()
runPeer cfg peerIdx knownPeers = do
  let FLConfig{..} = cfg
      peerPort = cfgBasePort
      peerId   = "peer-" <> show peerIdx

  rt      <- initRuntime (NodeAddr "localhost" (fromIntegral peerPort)) createTCPTransport
  doneVar <- newEmptyMVar

  withRuntime rt $ do
    forM_ knownPeers $ \addr -> retryConnect 20 addr
    liftIO $ threadDelay 300_000

    logRef <- spawnActor loggerHandler stopOnDeath ()
    registerActor (peerId <> "-logger") logRef

    initW <- liftIO initWeights
    let initModel = GlobalModel 0 (Weights (flattenWeights initW))

    (trainData, testData) <- liftIO $ loadAndPartition cfg peerIdx

    -- Shared TMVar: filled with the PeerCoordinator ref after it spawns.
    -- Evaluator and Trainer read from it lazily on their first message.
    pcCell <- liftIO newEmptyTMVarIO

    evalRef <- spawnActor p2pEvalHandler stopOnDeath
      (P2PEvalState testData pcCell logRef)

    trainerRef <- spawnActor p2pTrainerHandler stopOnDeath
      P2PTrainerState
        { p2tId        = peerId
        , p2tData      = trainData
        , p2tLastRound = -1
        , p2tPcCell    = pcCell
        , p2tLogRef    = logRef
        , p2tConfig    = cfg
        }

    -- PeerCoordinator — placeholder self-ref (replaced by SetPeers)
    let dummyId  = ActorId 0 (read "00000000-0000-0000-0000-000000000002")
        dummySelf = RemoteRef dummyId :: ActorRef PeerCoordinatorMsg ()

    let pcSt = PeerCoordinatorState
          { pcsNodeId       = peerId
          , pcsRoundCounter = gcEmpty
          , pcsParticipants = orEmpty
          , pcsCurrentRound = 0
          , pcsTotalRounds  = cfgRounds
          , pcsGlobalModel  = initModel
          , pcsPeerRefs     = []
          , pcsSelfRef      = dummySelf
          , pcsTrainerRef   = trainerRef
          , pcsEvalRef      = evalRef
          , pcsLogRef       = logRef
          , pcsCollected    = []
          , pcsNumPeers     = cfgNumTrainers
          , pcsDoneVar      = doneVar
          }

    -- Supervised PeerCoordinator. On every (re)spawn the shared cell is
    -- refreshed and the registry name re-registered, so trainer/evaluator
    -- keep reaching the live incarnation. (State restarts fresh.)
    supervise' OneForOne
      [ ChildSpec
          { csRun = \target -> do
              ref <- spawnActor peerCoordinatorHandler peerCoordinatorDeathHandler pcSt
              linkActorTo target ref
              return ref
          , csOnSpawn = \ref -> do
              liftIO $ atomically $ do
                void $ tryTakeTMVar pcCell
                putTMVar pcCell ref
              registerActor ("peercoord-" <> peerId) ref
          }
      ]
    pcRef <- liftIO $ atomically $ readTMVar pcCell

    logFn' logRef peerId INFO $
      "Peer ready, waiting for " <> show (cfgNumTrainers - 1) <> " other peers"

    -- Discover all other peer coordinators
    let otherIds = ["peer-" <> show i | i <- [0 .. cfgNumTrainers - 1], i /= peerIdx]
    peerPcRefs <- forM otherIds $ \pid ->
      retryLookup 40
        (lookupRemoteActor ("peercoord-" <> pid)
          :: RuntimeM (Maybe (ActorRef PeerCoordinatorMsg ())))

    -- Wire up PeerCoordinator with peers
    let peerIds = map actorRefId peerPcRefs
    cast' (SetPeers peerIds) pcRef

    liftIO $ threadDelay 200_000
    cast' PeerStartRound pcRef
    logFn' logRef peerId INFO "P2P round 1 started"

  takeMVar doneVar
  threadDelay 300_000
  putStrLn $ "\n=== Peer " <> show peerIdx <> " federation complete ==="
