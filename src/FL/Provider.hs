{-# LANGUAGE RecordWildCards #-}
module FL.Provider
  ( runCoordinator
  , runTrainer
  ) where

import Control.Actor
import Control.Actor.Registry (lookupRemoteActor, registerActor)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar)
import Control.Concurrent.STM
  ( atomically, newEmptyTMVarIO, putTMVar, readTMVar, tryTakeTMVar )
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict qualified as Map
import FL.Actors.Aggregator
import FL.Actors.Coordinator
import FL.Actors.Evaluator
import FL.Actors.Logger
import FL.Actors.Trainer
import FL.CRDT (orEmpty)
import FL.Data.PaySim (loadAndPartition)
import FL.NN (flattenWeights, initWeights)
import FL.Retry (retryConnect, retryLookup)
import FL.Types

-- ---------------------------------------------------------------------------
-- Coordinator node
-- ---------------------------------------------------------------------------

runCoordinator :: FLConfig -> IO ()
runCoordinator cfg = do
  let FLConfig{..} = cfg
  rt      <- initRuntime (NodeAddr "localhost" (fromIntegral cfgBasePort)) createTCPTransport
  doneVar <- newEmptyMVar

  withRuntime rt $ do
    -- Logger
    logRef <- spawnActor loggerHandler stopOnDeath ()
    registerActor "logger" logRef

    -- Initial global model
    initW <- liftIO initWeights
    let initModel = GlobalModel 0 (Weights (flattenWeights initW))

    -- Shared TMVar for coordinator ref: filled after coordinator spawns.
    -- Aggregator and Evaluator read from it lazily.
    coordCell <- liftIO newEmptyTMVarIO

    -- Load test data (coordinator holds the shared test split)
    (_, testData) <- liftIO $ loadAndPartition cfg 0

    -- Evaluator
    evalRef <- spawnActor evaluatorHandler stopOnDeath
      (EvaluatorState testData coordCell logRef)
    registerActor "evaluator" evalRef

    -- Aggregator
    aggRef <- spawnActor aggregatorHandler stopOnDeath
      (AggregatorState cfgNumTrainers [] coordCell logRef 0)
    registerActor "aggregator" aggRef

    -- Coordinator (knows aggRef and evalRef at spawn time)
    let initCoordSt = CoordinatorState
          { csTrainers         = orEmpty
          , csTrainerRefs      = Map.empty
          , csExpectedTrainers = cfgNumTrainers
          , csCurrentRound     = 0
          , csTotalRounds      = cfgRounds
          , csGlobalModel      = initModel
          , csAggRef           = aggRef
          , csEvalRef          = evalRef
          , csLogRef           = logRef
          , csDoneVar          = doneVar
          }
    -- Supervised coordinator. On every (re)spawn — including restarts — the
    -- shared cell is refreshed and the registry name re-registered, so
    -- aggregator/evaluator/trainers keep reaching the live incarnation.
    -- (Coordinator state itself restarts fresh: round progress is lost.)
    supervise' OneForOne
      [ ChildSpec
          { csRun = \target -> do
              ref <- spawnActor coordinatorHandler coordinatorDeathHandler initCoordSt
              linkActorTo target ref
              return ref
          , csOnSpawn = \ref -> do
              liftIO $ atomically $ do
                void $ tryTakeTMVar coordCell
                putTMVar coordCell ref
              registerActor "coordinator" ref
          }
      ]
    void $ liftIO $ atomically $ readTMVar coordCell

    logFn' logRef "System" INFO $
      "Coordinator ready, waiting for " <> show cfgNumTrainers <> " trainers"

  takeMVar doneVar
  threadDelay 300_000
  putStrLn "\n=== Provider federation complete ==="

-- ---------------------------------------------------------------------------
-- Trainer node
-- ---------------------------------------------------------------------------

runTrainer :: FLConfig -> Int -> NodeAddr -> IO ()
runTrainer cfg trainerIdx coordAddr = do
  let FLConfig{..} = cfg
  let trainerPort = cfgBasePort + trainerIdx + 1
      trainerId   = "trainer-" <> show trainerIdx

  rt <- initRuntime (NodeAddr "localhost" (fromIntegral trainerPort)) createTCPTransport

  withRuntime rt $ do
    retryConnect 20 coordAddr
    liftIO $ threadDelay 500_000  -- wait for registry sync

    -- Look up aggregator and logger on the coordinator node (retry until
    -- available; the logger is registered first, so if the aggregator
    -- resolves the logger will too)
    aggRef <- retryLookup 20
                (lookupRemoteActor "aggregator" :: RuntimeM (Maybe (ActorRef AggregatorMsg ())))
    logRef <- retryLookup 20
                (lookupRemoteActor "logger" :: RuntimeM (Maybe (ActorRef LogMsg ())))

    (trainData, _) <- liftIO $ loadAndPartition cfg trainerIdx

    let trainerSt = TrainerState
          { tsId        = trainerId
          , tsData      = trainData
          , tsLastRound = -1
          , tsAggRef    = aggRef
          , tsLogRef    = logRef
          , tsConfig    = cfg
          }
    trainerRef <- spawnActor trainerHandler stopOnDeath trainerSt
    registerActor trainerId trainerRef

    -- Register with coordinator
    coordRef <- retryLookup 20
                  (lookupRemoteActor "coordinator" :: RuntimeM (Maybe (ActorRef CoordinatorMsg ())))
    cast' (RegisterTrainer trainerId (actorRefId trainerRef)) coordRef

    logFn' logRef trainerId INFO "Registered with coordinator"

  waitForever

waitForever :: IO ()
waitForever = threadDelay maxBound >> waitForever
