module FL.Actors.Aggregator
  ( AggregatorState (..)
  , aggregatorHandler
  ) where

import Control.Actor.Core
import Control.Actor.Types
import Control.Concurrent.STM (TMVar, atomically, readTMVar)
import Control.Monad.IO.Class (liftIO)
import Data.List (sortOn)
import FL.Actors.Logger (logFn)
import FL.NN (fedAvg)
import FL.Types

data AggregatorState = AggregatorState
  { asExpected     :: Int
  , asUpdates      :: [ModelUpdate]
  , asCoordCell    :: TMVar (ActorRef CoordinatorMsg ())  -- filled after coordinator spawns
  , asLogRef       :: ActorRef LogMsg ()
  , asCurrentRound :: Int
  }

aggregatorHandler :: Handler AggregatorMsg AggregatorState ()
aggregatorHandler (SubmitUpdate upd) = do
  st <- state
  if muRound upd /= asCurrentRound st
    then pass
    else do
      let updates' = upd : filter (\u -> muTrainerId u /= muTrainerId upd) (asUpdates st)
      if length updates' >= asExpected st
        then do
          let pairs    = [(muSampleSize u, wData (muWeights u)) | u <- sortOn muTrainerId updates']
              avgW     = Weights (fedAvg pairs)
              nextRound = asCurrentRound st + 1
              newModel  = GlobalModel nextRound avgW
          logFn (asLogRef st) "Aggregator" INFO $
            "Round " <> show (asCurrentRound st)
            <> ": aggregated " <> show (length updates') <> " updates"
          coordRef <- liftIO $ atomically $ readTMVar (asCoordCell st)
          cast (AggregationComplete newModel) coordRef
          passWith st { asUpdates = [], asCurrentRound = nextRound }
        else
          passWith st { asUpdates = updates' }
