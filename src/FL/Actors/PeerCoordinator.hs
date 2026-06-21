{-# LANGUAGE StrictData #-}

module FL.Actors.PeerCoordinator
  ( PeerCoordinatorState (..)
  , peerCoordinatorHandler
  , peerCoordinatorDeathHandler
  ) where

import Control.Actor.Core
import Control.Actor.Types
import Data.Binary (encodeFile)
import Unsafe.Coerce (unsafeCoerce)
import Control.Concurrent.MVar (MVar, putMVar)
import Control.Monad (forM_, when)
import Control.Monad.IO.Class (liftIO)
import Data.List (sortOn)
import Data.Set qualified as Set
import Data.UUID.V4 (nextRandom)
import FL.Actors.Logger (logFn)
import FL.CRDT
import FL.NN (fedAvg)
import FL.Types

data PeerCoordinatorState = PeerCoordinatorState
  { pcsNodeId       :: String
  , pcsRoundCounter :: GCounter        -- completed training rounds per peer (monotone, never reset)
  , pcsParticipants :: ORSet String    -- active peers ever seen (monotone, never reset)
  , pcsCurrentRound :: Int
  , pcsTotalRounds  :: Int
  , pcsGlobalModel  :: GlobalModel
  , pcsPeerRefs     :: [ActorRef PeerCoordinatorMsg ()]
  , pcsSelfRef      :: ActorRef PeerCoordinatorMsg ()
  , pcsTrainerRef   :: ActorRef TrainerMsg ()
  , pcsEvalRef      :: ActorRef EvaluatorMsg ()
  , pcsLogRef       :: ActorRef LogMsg ()
  , pcsCollected    :: [ModelUpdate]   -- updates for the current round + buffered future rounds
  , pcsNumPeers     :: Int  -- total number of peers (including self)
  , pcsDoneVar      :: MVar ()
  }

-- | Insert an update, deduplicating on (trainer, round). Stale (past-round)
-- updates are dropped; future-round updates from fast peers are buffered so
-- they are not lost — without the round key a fast peer's round-(r+1) update
-- would overwrite its round-r update in a slower peer and rounds would mix.
insertUpdate :: Int -> ModelUpdate -> [ModelUpdate] -> [ModelUpdate]
insertUpdate currentRound upd collected
  | muRound upd < currentRound = collected
  | otherwise =
      upd : filter (\u -> (muTrainerId u, muRound u) /= (muTrainerId upd, muRound upd)) collected

currentRoundUpdates :: PeerCoordinatorState -> [ModelUpdate]
currentRoundUpdates st = [u | u <- pcsCollected st, muRound u == pcsCurrentRound st]

-- | A round is complete when the CRDTs agree with the collected updates:
-- the OR-Set has seen all peers, the G-Counter shows every one of them has
-- finished training the current round, and we hold all their updates.
roundComplete :: PeerCoordinatorState -> Bool
roundComplete st =
  let r       = pcsCurrentRound st
      members = orMembers (pcsParticipants st)
      trained = Set.filter (\p -> gcGet p (pcsRoundCounter st) > r) members
  in Set.size members >= pcsNumPeers st
     && Set.size trained >= pcsNumPeers st
     && length (currentRoundUpdates st) >= pcsNumPeers st

peerCoordinatorHandler :: Handler PeerCoordinatorMsg PeerCoordinatorState ()
peerCoordinatorHandler (SetPeers peerIds) = do
  st <- state
  (SomeActorRef self) <- getSelf
  let peers   = map (\aid -> RemoteRef aid :: ActorRef PeerCoordinatorMsg ()) peerIds
      selfRef = unsafeCoerce self :: ActorRef PeerCoordinatorMsg ()
  passWith st { pcsPeerRefs = peers, pcsSelfRef = selfRef }

peerCoordinatorHandler PeerStartRound = do
  st <- state
  logFn (pcsLogRef st) (pcsNodeId st) INFO $
    "Starting local training for P2P round " <> show (pcsCurrentRound st + 1)
  cast (StartTraining (pcsGlobalModel st)) (pcsTrainerRef st)
  pass

peerCoordinatorHandler (PeerTrainingDone upd) = do
  st <- state
  uuid <- liftIO nextRandom
  let counter'      = gcIncrement (pcsNodeId st) (pcsRoundCounter st)
      participants' = orAdd uuid (pcsNodeId st) (pcsParticipants st)
      collected'    = insertUpdate (pcsCurrentRound st) upd (pcsCollected st)
      payload       = PeerSyncPayload
                        { pspCounter      = counter'
                        , pspParticipants = participants'
                        , pspUpdate       = Just upd
                        }
  logFn (pcsLogRef st) (pcsNodeId st) INFO
    "Training done, broadcasting PeerSync"
  forM_ (pcsPeerRefs st) $ \ref ->
    cast (PeerSync payload) ref
  let st' = st { pcsRoundCounter = counter'
               , pcsParticipants = participants'
               , pcsCollected    = collected'
               }
  if roundComplete st'
    then finishRound st'
    else passWith st'

peerCoordinatorHandler (PeerSync payload) = do
  st <- state
  -- Merge CRDTs
  let counter'      = gcMerge (pcsRoundCounter st) (pspCounter payload)
      participants' = orMerge (pcsParticipants st) (pspParticipants payload)
      collected'    = case pspUpdate payload of
        Nothing  -> pcsCollected st
        Just upd -> insertUpdate (pcsCurrentRound st) upd (pcsCollected st)
      st' = st { pcsRoundCounter = counter'
               , pcsParticipants = participants'
               , pcsCollected    = collected'
               }
  if roundComplete st'
    then finishRound st'
    else passWith st'

peerCoordinatorHandler (PeerEvaluationDone metrics) = do
  st <- state
  logFn (pcsLogRef st) (pcsNodeId st) INFO $
    "Eval round=" <> show (mRound metrics)
    <> " acc="  <> show4 (mAccuracy  metrics)
    <> " auc="  <> show4 (mAUCROC    metrics)
  liftIO $ when (mRound metrics >= pcsTotalRounds st) $
    putMVar (pcsDoneVar st) ()
  pass
  where
    show4 x = show (fromIntegral (round (x * 10000) :: Int) / 10000.0 :: Double)

finishRound :: PeerCoordinatorState -> Actor PeerCoordinatorMsg PeerCoordinatorState ()
finishRound st = do
  -- Sort so every peer folds the weighted average in the same order and
  -- arrives at bit-identical models regardless of message arrival order.
  let updates   = sortOn muTrainerId (currentRoundUpdates st)
      pairs     = [(muSampleSize u, wData (muWeights u)) | u <- updates]
      avgW      = Weights (fedAvg pairs)
      nextRound = pcsCurrentRound st + 1
      newModel  = GlobalModel nextRound avgW
  logFn (pcsLogRef st) (pcsNodeId st) INFO $
    "P2P round " <> show (pcsCurrentRound st) <> " aggregated ("
    <> show (length updates) <> " updates)"
  cast (EvaluateModel newModel) (pcsEvalRef st)
  let st' = st
        { pcsGlobalModel  = newModel
        , pcsCurrentRound = nextRound
        -- Keep buffered updates from peers already in later rounds
        , pcsCollected    = [u | u <- pcsCollected st, muRound u >= nextRound]
        }
  if nextRound >= pcsTotalRounds st
    then do
      let filename = "fl_model_" <> pcsNodeId st' <> ".bin"
      liftIO $ encodeFile filename (gmWeights newModel)
      logFn (pcsLogRef st') (pcsNodeId st') INFO $
        "Model saved to " <> filename
      passWith st'
    else do
      -- Schedule next round after a short delay to let PeerSync propagate
      castIn 500 PeerStartRound (pcsSelfRef st')
      passWith st'

peerCoordinatorDeathHandler :: DeathMessage -> ActorM PeerCoordinatorState (SupervisorAction PeerCoordinatorState)
peerCoordinatorDeathHandler _ = do
  st <- state
  return (Continue st)
