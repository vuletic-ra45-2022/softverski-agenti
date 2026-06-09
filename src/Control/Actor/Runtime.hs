{-# LANGUAGE StrictData #-}

module Control.Actor.Runtime
  ( Runtime (..)
  , RuntimeM (..)
  , newRuntime
  , withRuntime
  , lookupNode
  ) where

import Control.Actor.Transport (Transport)
import Control.Actor.Types
import Control.Concurrent (ThreadId)
import Control.Concurrent.MVar (MVar)
import Control.Concurrent.STM
  ( TMVar, TVar, atomically, newTVarIO, readTVar )
import Control.Monad.Reader
  ( MonadIO (..), MonadReader (..), ReaderT (..), runReaderT )
import Data.ByteString.Lazy (ByteString)
import Data.Map qualified as Map

data Runtime = Runtime
  { rtNodeId       :: NodeAddr
  , rtNextNodeId   :: TVar NodeId
  , rtActors       :: TVar (Map.Map ActorId (ThreadId, SomeActorRef))
  , rtPending      :: TVar (Map.Map CorrelationId (MVar ByteString))
  , rtNextCorr     :: TVar CorrelationId
  , rtNodeTable    :: TVar (Map.Map NodeId NodeAddr)
  , rtTransport    :: Transport
  , rtConnections  :: TVar (Map.Map NodeAddr (ActorRef NetworkMessage ()))
  , rtConnPromises :: TVar (Map.Map NodeAddr (TMVar (ActorRef NetworkMessage ())))
  , rtSendRemote   :: NodeAddr -> NetworkMessage -> RuntimeM ()
  }

newtype RuntimeM a = RuntimeM
  { unRuntimeM :: ReaderT Runtime IO a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader Runtime)

newRuntime :: NodeAddr -> Transport -> IO Runtime
newRuntime myAddr transport = do
  actors    <- newTVarIO Map.empty
  pending   <- newTVarIO Map.empty
  nextCorr  <- newTVarIO (0 :: Integer)
  nodeTable <- newTVarIO Map.empty
  nextNid   <- newTVarIO (1 :: NodeId)
  conns     <- newTVarIO Map.empty
  promises  <- newTVarIO Map.empty
  return Runtime
    { rtNodeId       = myAddr
    , rtNextNodeId   = nextNid
    , rtActors       = actors
    , rtPending      = pending
    , rtNextCorr     = nextCorr
    , rtNodeTable    = nodeTable
    , rtTransport    = transport
    , rtConnections  = conns
    , rtConnPromises = promises
    , rtSendRemote   = \_ _ -> return ()
    }

withRuntime :: Runtime -> RuntimeM a -> IO a
withRuntime rt m = runReaderT (unRuntimeM m) rt
{-# INLINE withRuntime #-}

lookupNode :: NodeId -> RuntimeM (Maybe NodeAddr)
lookupNode nodeId = do
  rt <- ask
  liftIO $ atomically $ do
    table <- readTVar (rtNodeTable rt)
    return $ Map.lookup nodeId table
