{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE StrictData #-}

module Control.Actor.Types
  ( NodeAddr (..)
  , NodeId
  , thisNodeId
  , ActorId (..)
  , ExitReason (..)
  , DeathMessage (..)
  , DeathTarget (..)
  , RemoteExitReason (..)
  , toRemoteExitReason
  , NetworkMessage (..)
  , ActorState (..)
  , ActorRef (..)
  , SomeActorRef (..)
  , someActorId
  , actorRefId
  , SupervisorAction (..)
  , CorrelationId
  , Envelope (..)
  , RegistryMsg (..)
  , findByUUID
  ) where

import Control.Concurrent.MVar (MVar)
import Control.Concurrent.STM (TQueue, TVar)
import Control.Exception (SomeException)
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.UUID (UUID)
import GHC.Generics (Generic)
import Data.Map qualified as Map
import Control.Concurrent (ThreadId)

data NodeAddr = NodeAddr
  { nodeHost :: String
  , nodePort :: Integer
  } deriving (Eq, Ord, Show, Generic)

instance Binary NodeAddr

type NodeId = Integer

thisNodeId :: NodeId
thisNodeId = 0

data ActorId = ActorId NodeId UUID
  deriving (Eq, Ord, Show, Generic)

instance Binary ActorId

data ExitReason
  = Normal
  | Killed
  | Exception SomeException
  deriving Show

data DeathMessage = DeathMessage
  { dmActorId :: ActorId
  , dmReason  :: ExitReason
  } deriving Show

data DeathTarget
  = LocalTarget (TQueue DeathMessage)
  | RemoteTarget ActorId NodeId

data RemoteExitReason
  = RNormal
  | RKilled
  | RException String
  deriving (Show, Generic)

instance Binary RemoteExitReason

toRemoteExitReason :: ExitReason -> RemoteExitReason
toRemoteExitReason Normal        = RNormal
toRemoteExitReason Killed        = RKilled
toRemoteExitReason (Exception e) = RException (show e)

data NetworkMessage
  = NMHandshake NodeAddr
  | NMCast UUID ByteString
  | NMCall UUID CorrelationId NodeAddr ByteString
  | NMReply CorrelationId ByteString
  | NMDeath ActorId RemoteExitReason
  deriving (Generic, Show)

instance Binary NetworkMessage

data ActorState u = ActorState
  { asId    :: ActorId
  , asLinks :: TVar [DeathTarget]
  , asEnv   :: u
  , asLatestFrom :: NodeId
  }

type CorrelationId = Integer

data Envelope msg reply
  = Cast msg
  | Call msg (MVar (Maybe reply))

data ActorRef msg reply
  = forall u. LocalRef
      { arMsgQ   :: TQueue (NodeId, Envelope msg reply)
      , arDeathQ :: TQueue DeathMessage
      , arState  :: ActorState u
      }
  | RemoteRef ActorId

data SomeActorRef = forall msg reply. (Binary msg, Binary reply) => SomeActorRef (ActorRef msg reply)

someActorId :: SomeActorRef -> ActorId
someActorId (SomeActorRef (LocalRef {arState})) = asId arState
someActorId (SomeActorRef (RemoteRef aid))      = aid

actorRefId :: ActorRef msg r -> ActorId
actorRefId (LocalRef {arState}) = asId arState
actorRefId (RemoteRef aid)      = aid

data SupervisorAction u
  = Stop
  | Continue u

data RegistryMsg
  = RMRegister        String UUID
  | RMLookup          String
  | RMDeregister      String
  | RMDeath           UUID
  | RMDeregisterNode  NodeId
  deriving (Generic)

instance Binary RegistryMsg

findByUUID :: UUID -> Map.Map ActorId (ThreadId, SomeActorRef) -> Maybe SomeActorRef
findByUUID uuid actors =
  case Map.toList (Map.filterWithKey (\(ActorId _ u) _ -> u == uuid) actors) of
    []            -> Nothing
    (_, (_, r)):_ -> Just r
