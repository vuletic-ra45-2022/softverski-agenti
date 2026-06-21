module FL.Actors.Trainer
  ( TrainerState (..)
  , trainerHandler
  ) where

import Control.Actor.Core
import Control.Actor.Types
import Control.Monad.IO.Class (liftIO)
import FL.Actors.Logger (logFn)
import FL.NN (LocalData, flattenWeights, localDataSize, trainLocal, unflattenWeights)
import FL.Types

data TrainerState = TrainerState
  { tsId        :: String
  , tsData      :: LocalData
  , tsLastRound :: Int
  , tsAggRef    :: ActorRef AggregatorMsg ()
  , tsLogRef    :: ActorRef LogMsg ()
  , tsConfig    :: FLConfig
  }

trainerHandler :: Handler TrainerMsg TrainerState ()
trainerHandler (StartTraining gm) = do
  st <- state
  let round_ = gmRound gm
  if round_ <= tsLastRound st
    then pass  -- idempotent: already processed this round
    else do
      logFn (tsLogRef st) (tsId st) INFO $
        "Starting local training for round " <> show round_
      let initW = unflattenWeights (wData (gmWeights gm))
      updatedW <- liftIO $ trainLocal (tsConfig st) (tsId st) round_ initW (tsData st)
      let n      = localDataSize (tsData st)
          update = ModelUpdate
            { muTrainerId  = tsId st
            , muRound      = round_
            , muSampleSize = n
            , muWeights    = Weights (flattenWeights updatedW)
            }
      logFn (tsLogRef st) (tsId st) INFO $
        "Round " <> show round_ <> " training complete, samples=" <> show n
      cast (SubmitUpdate update) (tsAggRef st)
      passWith st { tsLastRound = round_ }

trainerHandler TrainerStop = pass
