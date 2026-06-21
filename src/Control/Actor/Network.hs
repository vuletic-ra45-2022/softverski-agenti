{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StrictData #-}

module Control.Actor.Network
  ( spawnConnTree
  , handleNewConn
  , getOrCreateConn
  , connect
  , findByUUID
  , routeRemoteDeath
  , validateAndDispatch
  , sysHandlerFn
  , createTCPTransport
  ) where

import Control.Actor.Core
  ( ActorM, ActorResult (..), Handler, cast', liftRuntime, linkActorTo
  , pass, spawnActor, state, stopOnDeath )
import Control.Actor.Registry (deregisterNode)
import Control.Actor.Runtime (Runtime (..), RuntimeM, addrToNodeId, withRuntime)
import Control.Actor.Transport (ConnHandle (..), Transport (..), createTCPTransport)
import Control.Actor.Types
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM
  ( atomically
  , modifyTVar
  , newEmptyTMVar
  , readTMVar
  , readTVar
  , readTVarIO
  , tryPutTMVar
  , writeTQueue
  , writeTVar
  )
import Control.Exception (SomeException, throwIO, toException, try)
import Control.Monad (forM_, forever, unless, void, when)
import Control.Monad.Reader (MonadIO (..), MonadReader (..), asks)
import Data.Binary (decode, decodeOrFail, encode)
import Data.ByteString.Lazy (ByteString)
import Data.Map qualified as Map
import Data.Maybe (fromMaybe)
import System.Timeout (timeout)

-- Connection actors

connActorFn :: Handler NetworkMessage ConnHandle ()
connActorFn nm = do
  ch <- state
  liftIO $ chSend ch (encode nm)
  return $ ActorResult Nothing ch Nothing

connDeathFn :: NodeAddr -> DeathMessage -> ActorM ConnHandle (SupervisorAction ConnHandle)
connDeathFn peer _ = do
  ch  <- state
  aid <- asks (asId . fst)
  rt  <- liftRuntime ask
  -- Only clear the pool entry if it is ours: with simultaneous connects two
  -- conn actors can exist for the same peer, and the loser must not evict
  -- the active one.
  wasActive <- liftIO $ atomically $ do
    conns <- readTVar (rtConnections rt)
    case Map.lookup peer conns of
      Just ref | actorRefId ref == aid -> do
        modifyTVar (rtConnections rt) (Map.delete peer)
        return True
      _else -> return False
  liftIO $ chClose ch
  when wasActive $ do
    mNodeId <- liftRuntime $ addrToNodeId peer
    forM_ mNodeId $ \nodeId -> liftRuntime $ deregisterNode nodeId
  return Stop

routerActorFn :: NodeAddr -> Handler ByteString () ()
routerActorFn senderAddr raw =
  case decodeOrFail raw of
    Left _ -> do
      liftIO $ putStrLn $ "router: dropping undecodable frame from " <> show senderAddr
      pass
    Right (_, _, nm) -> do
      valid <- liftRuntime $ validateAndDispatch senderAddr nm
      unless valid $ liftIO $
        putStrLn $ "router: dropping invalid message from " <> show senderAddr
      pass

-- Connection supervision tree

spawnConnTree :: NodeAddr -> ConnHandle -> RuntimeM (ActorRef NetworkMessage ())
spawnConnTree peer ch = do
  rt        <- ask
  routerRef <- spawnActor (routerActorFn peer) stopOnDeath ()
  connRef   <- spawnActor connActorFn (connDeathFn peer) ch
  -- When conn dies (peer disconnected), notify router so it shuts down too
  case routerRef of
    LocalRef {arDeathQ} -> linkActorTo (LocalTarget arDeathQ) connRef
    RemoteRef _         -> return ()
  -- And vice versa: if the router dies, close the connection down cleanly
  case connRef of
    LocalRef {arDeathQ} -> linkActorTo (LocalTarget arDeathQ) routerRef
    RemoteRef _         -> return ()
  liftIO $ case connRef of
    LocalRef {arDeathQ, arState} ->
      void $ forkIO $ do
        result <- try @SomeException $ forever $
          chRecv ch >>= \raw -> withRuntime rt $ cast' raw routerRef
        case result of
          Left _   -> atomically $
            writeTQueue arDeathQ (DeathMessage (asId arState) Killed)
          Right () -> return ()
    RemoteRef _ -> return ()
  return connRef

-- Incoming connection handler

handleNewConn :: ConnHandle -> RuntimeM ()
handleNewConn ch = do
  rt  <- ask
  raw <- liftIO $ chRecv ch
  case decode raw :: NetworkMessage of
    NMHandshake peerAddr -> do
      ref <- spawnConnTree peerAddr ch
      liftIO $ atomically $ do
        conns <- readTVar (rtConnections rt)
        -- On a simultaneous connect two sockets (and two conn trees) exist
        -- for the same peer. Only the pool entry is deduplicated; the extra
        -- tree must stay alive because it is the read path for whatever the
        -- peer sends on its socket. It cleans itself up when that socket
        -- closes. Local waiters get the pool winner, never the extra ref.
        let winner = Map.findWithDefault ref peerAddr conns
        unless (Map.member peerAddr conns) $
          modifyTVar (rtConnections rt) (Map.insert peerAddr ref)
        table <- readTVar (rtNodeTable rt)
        unless (any (== peerAddr) (Map.elems table)) $ do
          nid <- readTVar (rtNextNodeId rt)
          writeTVar (rtNextNodeId rt) (nid + 1)
          modifyTVar (rtNodeTable rt) (Map.insert nid peerAddr)
        promises <- readTVar (rtConnPromises rt)
        case Map.lookup peerAddr promises of
          Nothing -> return ()
          Just p  -> do
            modifyTVar (rtConnPromises rt) (Map.delete peerAddr)
            void $ tryPutTMVar p (Right winner)
    _else -> liftIO $ chClose ch

-- Connection pool

getOrCreateConn :: NodeAddr -> RuntimeM (ActorRef NetworkMessage ())
getOrCreateConn peer = do
  rt <- ask
  action <- liftIO $ atomically $ do
    conns <- readTVar (rtConnections rt)
    case Map.lookup peer conns of
      Just ref -> return (Left ref)
      Nothing  -> do
        promises <- readTVar (rtConnPromises rt)
        case Map.lookup peer promises of
          Just p  -> return (Right (Left p))
          Nothing -> do
            p <- newEmptyTMVar
            modifyTVar (rtConnPromises rt) (Map.insert peer p)
            return (Right (Right p))
  case action of
    Left ref              -> return ref
    Right (Left promise)  -> do
      -- Someone else is connecting; wait for their outcome so a failed
      -- connect propagates instead of blocking us forever.
      outcome <- liftIO $ atomically $ readTMVar promise
      either (liftIO . throwIO) return outcome
    Right (Right promise) -> do
      result <- liftIO $ try @SomeException $ tConnect (rtTransport rt) peer
      case result of
        Left e -> do
          liftIO $ atomically $ do
            modifyTVar (rtConnPromises rt) (Map.delete peer)
            void $ tryPutTMVar promise (Left e)
          liftIO $ throwIO e
        Right ch -> do
          liftIO $ chSend ch (encode (NMHandshake (rtNodeId rt)))
          ref <- spawnConnTree peer ch
          liftIO $ atomically $ do
            conns <- readTVar (rtConnections rt)
            unless (Map.member peer conns) $
              modifyTVar (rtConnections rt) (Map.insert peer ref)
            modifyTVar (rtConnPromises rt) (Map.delete peer)
            void $ tryPutTMVar promise (Right ref)
          return ref

-- Message dispatch

routeRemoteDeath :: NodeAddr -> ActorId -> RemoteExitReason -> RuntimeM ()
routeRemoteDeath senderAddr (ActorId _ uuid) reason = do
  rt           <- ask
  mSenderNid   <- addrToNodeId senderAddr
  liftIO $ do
    let deadId     = ActorId (fromMaybe 0 mSenderNid) uuid
        exitReason = case reason of
          RNormal      -> Normal
          RKilled      -> Killed
          RException s -> Exception (toException (userError s))
        dm = DeathMessage deadId exitReason
    actors <- readTVarIO (rtActors rt)
    forM_ (Map.elems actors) $ \(_, SomeActorRef ref) ->
      case ref of
        LocalRef {arDeathQ, arState} -> do
          links <- readTVarIO (asLinks arState)
          forM_ links $ \case
            RemoteTarget (ActorId _ uid) nodeId
              | uid == uuid && Just nodeId == mSenderNid ->
                  atomically $ writeTQueue arDeathQ dm
            _else -> return ()
        RemoteRef _ -> return ()

validateAndDispatch :: NodeAddr -> NetworkMessage -> RuntimeM Bool
validateAndDispatch senderAddr nm = case nm of

  NMHandshake _ -> return False

  NMReply corrId payload -> do
    rt <- ask
    liftIO $ do
      pending <- readTVarIO (rtPending rt)
      case Map.lookup corrId pending of
        Nothing -> return False
        Just mv -> putMVar mv payload >> return True

  NMCast uuid payload -> do
    rt <- ask
    rid <- remoteNodeId
    liftIO $ do
      actors <- readTVarIO (rtActors rt)
      case findByUUID uuid actors of
        Nothing -> do
          putStrLn $ "didn't find actor: " <> show uuid
          return False
        Just (SomeActorRef (LocalRef {arMsgQ})) ->
          case decodeOrFail payload of
            Left _            -> return False
            Right (_, _, msg) -> do
              atomically $ writeTQueue arMsgQ (rid, Cast msg)
              return True
        Just (SomeActorRef (RemoteRef _)) ->
          return False

  NMCall uuid corrId returnAddr payload -> do
    rt <- ask
    rid <- remoteNodeId
    liftIO $ do
      actors <- readTVarIO (rtActors rt)
      case findByUUID uuid actors of
        Nothing ->
          return False
        Just (SomeActorRef (LocalRef {arMsgQ})) ->
          case decodeOrFail payload of
            Left _            -> return False
            Right (_, _, msg) -> do
              mv <- newEmptyMVar
              atomically $ writeTQueue arMsgQ (rid, Call msg mv)
              void $ forkIO $ do
                -- Bounded wait: if the target actor dies before replying,
                -- this thread must not linger forever.
                reply <- timeout 30_000_000 (takeMVar mv)
                case reply of
                  Nothing        -> return ()
                  Just Nothing   -> return ()
                  Just (Just rv) -> void $ try @SomeException $
                    withRuntime rt $ do
                      connRef <- getOrCreateConn returnAddr
                      cast' (NMReply corrId (encode rv)) connRef
              return True
        Just (SomeActorRef (RemoteRef _)) ->
          return False

  NMDeath deadId reason -> do
    routeRemoteDeath senderAddr deadId reason
    return True

  where
    remoteNodeId = fromMaybe thisNodeId <$> addrToNodeId senderAddr

-- System event handler

sysHandlerFn :: Handler NetworkMessage () ()
sysHandlerFn _ = return $ ActorResult Nothing () Nothing

-- Node connection

-- | Connect to a remote node and return its locally-assigned NodeId.
-- NodeId 0 is always self; remote nodes get ids starting from 1.
-- A suggested id is honoured if it is free (non-zero, not already in use).
connect :: Maybe NodeId -> NodeAddr -> RuntimeM NodeId
connect suggestedId peer = do
  rt     <- ask
  nodeId <- liftIO $ atomically $ do
    table <- readTVar (rtNodeTable rt)
    case Map.foldrWithKey (\k v acc -> if v == peer then Just k else acc) Nothing table of
      Just existing -> return existing
      Nothing -> do
        nid <- readTVar (rtNextNodeId rt)
        let assigned = case suggestedId of
              Just n | n /= 0 && not (Map.member n table) -> n
              _else                                        -> nid
        writeTVar  (rtNextNodeId rt) (max (assigned + 1) nid)
        modifyTVar (rtNodeTable rt) (Map.insert assigned peer)
        return assigned
  void $ getOrCreateConn peer
  return nodeId
