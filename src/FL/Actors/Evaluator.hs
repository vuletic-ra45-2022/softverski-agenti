module FL.Actors.Evaluator
  ( EvaluatorState (..)
  , evaluatorHandler
  ) where

import Control.Actor.Core
import Control.Actor.Types
import Control.Concurrent.STM (TMVar, atomically, readTMVar)
import Control.Monad.IO.Class (liftIO)
import FL.Actors.Logger (logFn)
import FL.NN (LocalData, evaluate, unflattenWeights)
import FL.Types

data EvaluatorState = EvaluatorState
  { esTestData  :: LocalData
  , esCoordCell :: TMVar (ActorRef CoordinatorMsg ())  -- filled after coordinator spawns
  , esLogRef    :: ActorRef LogMsg ()
  }

evaluatorHandler :: Handler EvaluatorMsg EvaluatorState ()
evaluatorHandler (EvaluateModel gm) = do
  st <- state
  let ws      = unflattenWeights (wData (gmWeights gm))
      metrics = evaluate ws (esTestData st) (gmRound gm)
  logFn (esLogRef st) "Evaluator" INFO $
    "Round " <> show (gmRound gm)
    <> "  acc="  <> show4 (mAccuracy  metrics)
    <> "  prec=" <> show4 (mPrecision metrics)
    <> "  rec="  <> show4 (mRecall    metrics)
    <> "  f1="   <> show4 (mF1        metrics)
    <> "  auc="  <> show4 (mAUCROC    metrics)
  coordRef <- liftIO $ atomically $ readTMVar (esCoordCell st)
  cast (EvaluationDone metrics) coordRef
  pass
  where
    show4 x = show (fromIntegral (round (x * 10000) :: Int) / 10000.0 :: Double)
