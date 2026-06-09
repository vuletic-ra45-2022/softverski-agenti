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
  , SupervisorAction (..)
  , CorrelationId
  , Envelope (..)
  ) where

import Control.Concurrent.MVar (MVar)
import Control.Concurrent.STM (TQueue, TVar)
import Control.Exception (SomeException)
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.UUID (UUID)
import GHC.Generics (Generic)

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
  | RemoteTarget ActorId NodeAddr

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
  deriving (Generic)

instance Binary NetworkMessage

data ActorState u = ActorState
  { asId    :: ActorId
  , asLinks :: TVar [DeathTarget]
  , asEnv   :: u
  }

type CorrelationId = Integer

data Envelope msg reply
  = Cast msg
  | Call msg (MVar (Maybe reply))

data ActorRef msg reply
  = forall u. LocalRef
      { arMsgQ   :: TQueue (Envelope msg reply)
      , arDeathQ :: TQueue DeathMessage
      , arState  :: ActorState u
      }
  | RemoteRef ActorId

data SomeActorRef = forall msg reply. (Binary msg, Binary reply) => SomeActorRef (ActorRef msg reply)

someActorId :: SomeActorRef -> ActorId
someActorId (SomeActorRef (LocalRef {arState})) = asId arState
someActorId (SomeActorRef (RemoteRef aid))      = aid

data SupervisorAction u
  = Stop
  | Continue u
