module FL.Types
  ( -- Config
    FLConfig (..)
  , PartitionMode (..)
  , NormMode (..)
    -- Wire types
  , Weights (..)
  , GlobalModel (..)
  , ModelUpdate (..)
  , Metrics (..)
    -- Logger messages
  , LogLevel (..)
  , LogEntry (..)
  , LogMsg (..)
    -- Actor messages
  , EvaluatorMsg (..)
  , TrainerMsg (..)
  , AggregatorMsg (..)
  , CoordinatorMsg (..)
  , PeerSyncPayload (..)
  , PeerCoordinatorMsg (..)
  ) where

import Control.Actor.Types (ActorId)
import Data.Binary (Binary)
import FL.CRDT (GCounter, ORSet)
import GHC.Generics (Generic)

-- ---------------------------------------------------------------------------
-- Config (not a wire type — parsed from CLI, passed around locally)
-- ---------------------------------------------------------------------------

data PartitionMode = IID | NonIID
  deriving (Show, Eq, Generic)
instance Binary PartitionMode

data NormMode = MinMax | ZScore
  deriving (Show, Eq, Generic)
instance Binary NormMode

data FLConfig = FLConfig
  { cfgRounds       :: Int
  , cfgEpochs       :: Int
  , cfgLearningRate :: Double
  , cfgBatchSize    :: Int
  , cfgNumTrainers  :: Int
  , cfgDataPath     :: Maybe FilePath
  , cfgPartition    :: PartitionMode
  , cfgNorm         :: NormMode
  , cfgBasePort     :: Int
  } deriving (Show, Generic)

-- ---------------------------------------------------------------------------
-- Wire types (sent over the network, must be Binary)
-- ---------------------------------------------------------------------------

newtype Weights = Weights { wData :: [Double] }
  deriving (Show, Eq, Generic)
instance Binary Weights

data GlobalModel = GlobalModel
  { gmRound   :: Int
  , gmWeights :: Weights
  } deriving (Show, Eq, Generic)
instance Binary GlobalModel

data ModelUpdate = ModelUpdate
  { muTrainerId  :: String
  , muRound      :: Int
  , muSampleSize :: Int
  , muWeights    :: Weights
  } deriving (Show, Eq, Generic)
instance Binary ModelUpdate

data Metrics = Metrics
  { mAccuracy  :: Double
  , mPrecision :: Double
  , mRecall    :: Double
  , mF1        :: Double
  , mAUCROC    :: Double
  , mRound     :: Int
  } deriving (Show, Eq, Generic)
instance Binary Metrics

-- ---------------------------------------------------------------------------
-- Logger
-- ---------------------------------------------------------------------------

data LogLevel = DEBUG | INFO | WARN | ERROR
  deriving (Show, Eq, Ord, Generic)
instance Binary LogLevel

data LogEntry = LogEntry
  { leSource :: String
  , leLevel  :: LogLevel
  , leMsg    :: String
  } deriving (Show, Generic)
instance Binary LogEntry

data LogMsg = Log LogEntry
  deriving (Show, Generic)
instance Binary LogMsg

-- ---------------------------------------------------------------------------
-- Evaluator
-- ---------------------------------------------------------------------------

data EvaluatorMsg = EvaluateModel GlobalModel
  deriving (Show, Generic)
instance Binary EvaluatorMsg

-- ---------------------------------------------------------------------------
-- Trainer
-- ---------------------------------------------------------------------------

data TrainerMsg
  = StartTraining GlobalModel
  | TrainerStop
  deriving (Show, Generic)
instance Binary TrainerMsg

-- ---------------------------------------------------------------------------
-- Aggregator (provider mode)
-- ---------------------------------------------------------------------------

data AggregatorMsg = SubmitUpdate ModelUpdate
  deriving (Show, Generic)
instance Binary AggregatorMsg

-- ---------------------------------------------------------------------------
-- Coordinator (provider mode)
-- ---------------------------------------------------------------------------

data CoordinatorMsg
  = RegisterTrainer String ActorId
  | AggregationComplete GlobalModel
  | EvaluationDone Metrics
  | StartFederation
  deriving (Show, Generic)
instance Binary CoordinatorMsg

-- ---------------------------------------------------------------------------
-- PeerCoordinator (P2P mode)
-- ---------------------------------------------------------------------------

data PeerSyncPayload = PeerSyncPayload
  { pspCounter      :: !GCounter
  , pspParticipants :: !(ORSet String)
  , pspUpdate       :: !(Maybe ModelUpdate)
  } deriving (Show, Generic)
instance Binary PeerSyncPayload

data PeerCoordinatorMsg
  = SetPeers ![ActorId]
  | PeerStartRound
  | PeerTrainingDone !ModelUpdate
  | PeerSync !PeerSyncPayload
  | PeerEvaluationDone !Metrics
  deriving (Show, Generic)
instance Binary PeerCoordinatorMsg
