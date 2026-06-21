module FL.Actors.Coordinator
  ( CoordinatorState (..)
  , coordinatorHandler
  , coordinatorDeathHandler
  ) where

import Control.Actor.Core
import Control.Actor.Types
import Data.Binary (encodeFile)
import Control.Concurrent.MVar (MVar, putMVar)
import Control.Monad (forM_, when)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.UUID.V4 (nextRandom)
import FL.Actors.Logger (logFn)
import FL.CRDT (ORSet, orAdd, orMembers)
import FL.Types

data CoordinatorState = CoordinatorState
  { csTrainers         :: ORSet String
  , csTrainerRefs      :: Map String (ActorRef TrainerMsg ())
  , csExpectedTrainers :: Int
  , csCurrentRound     :: Int
  , csTotalRounds      :: Int
  , csGlobalModel      :: GlobalModel
  , csAggRef           :: ActorRef AggregatorMsg ()
  , csEvalRef          :: ActorRef EvaluatorMsg ()
  , csLogRef           :: ActorRef LogMsg ()
  , csDoneVar          :: MVar ()
  }

coordinatorHandler :: Handler CoordinatorMsg CoordinatorState ()
coordinatorHandler (RegisterTrainer tid aid) = do
  st <- state
  -- The ActorId in the message carries the *sender's* node numbering (its own
  -- node id is always 0 to itself); rebase it onto our node id for the sender,
  -- otherwise every cast to the trainer is dropped ("no node with id 0").
  from <- lastMessageFrom
  uuid <- liftIO nextRandom
  let ActorId _ trainerUUID = aid
      ref       = RemoteRef (ActorId from trainerUUID) :: ActorRef TrainerMsg ()
      trainers' = orAdd uuid tid (csTrainers st)
      refs'     = Map.insert tid ref (csTrainerRefs st)
      st'       = st { csTrainers = trainers', csTrainerRefs = refs' }
      numRegistered = Set.size (orMembers trainers')
  logFn (csLogRef st) "Coordinator" INFO $
    "Trainer registered: " <> tid
    <> " (" <> show numRegistered <> "/" <> show (csExpectedTrainers st) <> ")"
  -- Auto-start once all expected trainers have registered (per the OR-Set)
  if numRegistered >= csExpectedTrainers st
    then startRound st'
    else passWith st'

coordinatorHandler StartFederation = do
  st <- state
  startRound st

coordinatorHandler (AggregationComplete newModel) = do
  st <- state
  logFn (csLogRef st) "Coordinator" INFO $
    "Round " <> show (gmRound newModel - 1) <> " aggregation complete"
  cast (EvaluateModel newModel) (csEvalRef st)
  let st' = st { csGlobalModel = newModel, csCurrentRound = gmRound newModel }
  if csCurrentRound st' >= csTotalRounds st'
    then do
      liftIO $ encodeFile "fl_model_coordinator.bin" (gmWeights newModel)
      logFn (csLogRef st') "Coordinator" INFO
        "Model saved to fl_model_coordinator.bin"
      passWith st'
    else
      startRound st'

coordinatorHandler (EvaluationDone metrics) = do
  st <- state
  logFn (csLogRef st) "Coordinator" INFO $
    "Eval round=" <> show (mRound metrics)
    <> " acc="  <> show4 (mAccuracy  metrics)
    <> " prec=" <> show4 (mPrecision metrics)
    <> " rec="  <> show4 (mRecall    metrics)
    <> " f1="   <> show4 (mF1        metrics)
    <> " auc="  <> show4 (mAUCROC    metrics)
  liftIO $ when (mRound metrics >= csTotalRounds st) $
    putMVar (csDoneVar st) ()
  pass
  where
    show4 x = show (fromIntegral (round (x * 10000) :: Int) / 10000.0 :: Double)

startRound :: CoordinatorState -> Actor CoordinatorMsg CoordinatorState ()
startRound st = do
  let model = csGlobalModel st
  logFn (csLogRef st) "Coordinator" INFO $
    "Starting round " <> show (gmRound model + 1)
    <> " with " <> show (Map.size (csTrainerRefs st)) <> " trainers"
  forM_ (Map.elems (csTrainerRefs st)) $ \ref ->
    cast (StartTraining model) ref
  passWith st

coordinatorDeathHandler :: DeathMessage -> ActorM CoordinatorState (SupervisorAction CoordinatorState)
coordinatorDeathHandler _ = do
  st <- state
  return (Continue st)
