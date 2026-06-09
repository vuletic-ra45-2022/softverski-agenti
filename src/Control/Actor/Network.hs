{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StrictData #-}

module Control.Actor.Network
  ( spawnConnTree
  , handleNewConn
  , getOrCreateConn
  , findByUUID
  , routeRemoteDeath
  , validateAndDispatch
  , sysHandlerFn
  , initRuntime
  ) where

import Control.Actor.Core
  ( Actor, ActorM, cast', liftRuntime, linkActorTo, spawnActor, state, stopOnDeath )
import Control.Actor.Runtime (Runtime (..), RuntimeM, newRuntime, withRuntime)
import Control.Actor.Supervision (ChildSpec (..), RestartStrategy (..), childWithRef, supervise')
import Control.Actor.Transport (ConnHandle (..), Transport (..), createTCPTransport)
import Control.Actor.Types
import Control.Concurrent (ThreadId, forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM
  ( atomically
  , modifyTVar
  , newEmptyTMVar
  , newEmptyTMVarIO
  , putTMVar
  , readTMVar
  , readTVar
  , readTVarIO
  , tryTakeTMVar
  , writeTQueue
  )
import Control.Exception (SomeException, try)
import Control.Monad (forM_, forever, unless, void)
import Control.Monad.Reader (MonadIO (..), MonadReader (..))
import Data.Binary (decode, decodeOrFail, encode)
import Data.ByteString.Lazy (ByteString)
import Data.Map qualified as Map
import Data.UUID (UUID)

-- Connection actors

connActorFn :: NetworkMessage -> Actor ConnHandle ()
connActorFn nm = do
  ch <- state
  liftIO $ chSend ch (encode nm)
  return (Nothing, ch)

connDeathFn :: NodeAddr -> DeathMessage -> ActorM ConnHandle (SupervisorAction ConnHandle)
connDeathFn peer _ = do
  ch <- state
  rt <- liftRuntime ask
  liftIO $ do
    atomically $ modifyTVar (rtConnections rt) (Map.delete peer)
    chClose ch
  return Stop

routerActorFn :: ByteString -> Actor () ()
routerActorFn raw = do
  let nm = decode raw :: NetworkMessage
  valid <- liftRuntime $ validateAndDispatch nm
  unless valid $ liftIO $ putStrLn "router: dropping invalid message"
  return (Nothing, ())

-- Connection supervision tree

spawnConnTree :: NodeAddr -> ConnHandle -> RuntimeM (ActorRef NetworkMessage ())
spawnConnTree peer ch = do
  rt         <- ask
  routerCell <- liftIO newEmptyTMVarIO
  connCell   <- liftIO newEmptyTMVarIO
  supervise' OneForAll
    [ childWithRef routerActorFn stopOnDeath () routerCell
    , ChildSpec
        { csRun = \target -> do
            ref <- spawnActor connActorFn (connDeathFn peer) ch
            linkActorTo target ref
            return ref
        , csOnSpawn = \ref -> case ref of
            LocalRef { arDeathQ, arState } -> do
              atomically $ do
                void $ tryTakeTMVar connCell
                putTMVar connCell ref
              routerRef <- atomically $ readTMVar routerCell
              void $ forkIO $ do
                result <- try @SomeException $ forever $
                  chRecv ch >>= \raw -> withRuntime rt $ cast' raw routerRef
                case result of
                  Left _   -> atomically $
                    writeTQueue arDeathQ (DeathMessage (asId arState) Killed)
                  Right () -> return ()
            RemoteRef _ -> return ()
        }
    ]
  liftIO $ atomically $ readTMVar connCell

-- Incoming connection handler

handleNewConn :: ConnHandle -> RuntimeM ()
handleNewConn ch = do
  rt  <- ask
  raw <- liftIO $ chRecv ch
  case decode raw :: NetworkMessage of
    NMHandshake peerAddr -> do
      ref <- spawnConnTree peerAddr ch
      liftIO $ atomically $ do
        modifyTVar (rtConnections rt) (Map.insert peerAddr ref)
        promises <- readTVar (rtConnPromises rt)
        case Map.lookup peerAddr promises of
          Nothing -> return ()
          Just p  -> do
            modifyTVar (rtConnPromises rt) (Map.delete peerAddr)
            putTMVar p ref
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
    Right (Left promise)  -> liftIO $ atomically $ readTMVar promise
    Right (Right promise) -> do
      ch <- liftIO $ tConnect (rtTransport rt) peer
      liftIO $ chSend ch (encode (NMHandshake (rtNodeId rt)))
      ref <- spawnConnTree peer ch
      liftIO $ atomically $ do
        modifyTVar (rtConnections rt) (Map.insert peer ref)
        modifyTVar (rtConnPromises rt) (Map.delete peer)
        putTMVar promise ref
      return ref

-- Message dispatch

findByUUID :: UUID -> Map.Map ActorId (ThreadId, SomeActorRef) -> Maybe SomeActorRef
findByUUID uuid actors =
  case Map.toList (Map.filterWithKey (\(ActorId _ u) _ -> u == uuid) actors) of
    []            -> Nothing
    (_, (_, r)):_ -> Just r

routeRemoteDeath :: ActorId -> RemoteExitReason -> RuntimeM ()
routeRemoteDeath deadId reason = do
  rt <- ask
  liftIO $ do
    actors <- readTVarIO (rtActors rt)
    let exitReason = case reason of
          RNormal      -> Normal
          RKilled      -> Killed
          RException s -> Exception (error s)
        dm = DeathMessage deadId exitReason
    forM_ (Map.elems actors) $ \(_, SomeActorRef ref) ->
      case ref of
        LocalRef {arDeathQ, arState} -> do
          links <- readTVarIO (asLinks arState)
          forM_ links $ \case
            RemoteTarget rid _ | rid == deadId ->
              atomically $ writeTQueue arDeathQ dm
            _else -> return ()
        RemoteRef _ -> return ()

validateAndDispatch :: NetworkMessage -> RuntimeM Bool
validateAndDispatch nm = case nm of

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
    liftIO $ do
      actors <- readTVarIO (rtActors rt)
      case findByUUID uuid actors of
        Nothing ->
          return False
        Just (SomeActorRef (LocalRef {arMsgQ})) ->
          case decodeOrFail payload of
            Left _            -> return False
            Right (_, _, msg) -> do
              atomically $ writeTQueue arMsgQ (Cast msg)
              return True
        Just (SomeActorRef (RemoteRef _)) ->
          return False

  NMCall uuid corrId returnAddr payload -> do
    rt <- ask
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
              atomically $ writeTQueue arMsgQ (Call msg mv)
              void $ forkIO $ do
                reply <- takeMVar mv
                case reply of
                  Nothing -> return ()
                  Just rv -> withRuntime rt $ do
                    connRef <- getOrCreateConn returnAddr
                    cast' (NMReply corrId (encode rv)) connRef
              return True
        Just (SomeActorRef (RemoteRef _)) ->
          return False

  NMDeath deadId reason -> do
    routeRemoteDeath deadId reason
    return True

-- System event handler

sysHandlerFn :: NetworkMessage -> Actor () ()
sysHandlerFn (NMDeath deadId reason) = do
  liftRuntime $ routeRemoteDeath deadId reason
  return (Nothing, ())
sysHandlerFn _ = return (Nothing, ())

-- Runtime initialization

initRuntime :: NodeAddr -> IO Runtime
initRuntime myAddr = do
  transport <- createTCPTransport myAddr
  rt0       <- newRuntime myAddr transport
  let rt = rt0 { rtSendRemote = \addr nm -> getOrCreateConn addr >>= cast' nm }
  withRuntime rt $ void $ spawnActor sysHandlerFn stopOnDeath ()
  tListen transport $ \ch -> do
    result <- try @SomeException $ withRuntime rt $ handleNewConn ch
    case result of
      Left _   -> chClose ch
      Right () -> return ()
  return rt
