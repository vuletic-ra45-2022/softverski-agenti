module Control.Actor
  ( module Control.Actor.Types
  , module Control.Actor.Transport
  , module Control.Actor.Runtime
  , module Control.Actor.Core
  , module Control.Actor.Supervision
  , module Control.Actor.Network
  -- Demo
  , pingActor
  , forwardActorWithCell
  , repeatActor
  , system
  , networkDemo
  ) where

import Control.Actor.Core
import Control.Actor.Network
import Control.Actor.Runtime
import Control.Actor.Supervision
import Control.Actor.Transport
import Control.Actor.Types

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
  ( TMVar, TVar
  , atomically, newEmptyTMVarIO, newTVarIO, readTMVar, readTVarIO, writeTQueue, writeTVar
  )
import Control.Monad.Reader (MonadIO (..))
import Unsafe.Coerce (unsafeCoerce)

-- Demo

pingActor :: String -> Actor () String
pingActor msg = return (Just ("Hello, " <> msg <> "!"), ())

forwardActorWithCell :: TMVar (ActorRef String String) -> String -> Actor () String
forwardActorWithCell cell msg = do
  pingRef <- liftIO $ atomically $ readTMVar cell
  reply   <- call msg pingRef
  case reply of
    Nothing -> liftIO $ putStrLn "forwardActorWithCell: received empty reply!"
    Just x  -> liftIO $ putStrLn ("forwardActorWithCell: received reply - " <> x)
  return (reply, ())

repeatActor :: String -> TMVar (ActorRef String String) -> () -> Actor () String
repeatActor r cell () = do
  ref             <- liftIO $ atomically $ readTMVar cell
  (SomeActorRef self) <- getSelf
  cast r ref
  castIn 1000 () (unsafeCoerce self)
  return (Nothing, ())

system :: IO ()
system = do
  rt <- initRuntime (NodeAddr "localhost" 9000)
  withRuntime rt $ do
    pingCell    <- liftIO newEmptyTMVarIO
    forwardCell <- liftIO newEmptyTMVarIO
    repeatCell  <- liftIO newEmptyTMVarIO

    _ <- liftIO $ forkIO $ do
      repeatRef <- atomically $ readTMVar repeatCell
      withRuntime rt $
        cast' () repeatRef

    supervise'
      OneForOne
      [ childWithRef pingActor stopOnDeath () pingCell
      , childWithRef (forwardActorWithCell pingCell) stopOnDeath () forwardCell
      , childWithRef (repeatActor "repeaaat" forwardCell) stopOnDeath () repeatCell
      ]

-- Network demo

replyWith :: r -> Actor u r
replyWith x = do
  s <- state
  return (Just x, s)

pass :: Actor u r
pass = state >>= (return . (,) Nothing)


-- | Actor on node 2: echo — returns whatever it received.
echoActor :: String -> Actor () String
echoActor = replyWith

-- | Actor on node 2: printer — side-effects only.
printerActor :: String -> Actor () ()
printerActor msg = liftIO (putStrLn $ "  [remote/print] " <> msg) >> pass

-- | Trivial actor used as a stub when we only care about death notifications.
noopActor :: () -> Actor () ()
noopActor _ = return (Nothing, ())

-- | Captures the received string into a TVar (used as a node-1 receiver actor).
recvActorFn :: TVar (Maybe String) -> String -> Actor () ()
recvActorFn var msg =
  liftIO (atomically $ writeTVar var (Just msg)) >> return (Nothing, ())

-- | End-to-end networking demo.
--
-- Covers:
--   1. Remote cast  (node 1 → node 2)
--   2. Remote call  (node 1 ↔ node 2, request/reply)
--   3. Reverse cast (node 2 → node 1)
--   4. Cross-node death notification
networkDemo :: IO ()
networkDemo = do
  putStrLn "=== networking demo ==="

  -- Port 0 lets the OS pick a free port each run, avoiding stale-socket
  -- conflicts when re-running in the same GHCi session.
  rt1 <- initRuntime (NodeAddr "localhost" 0)
  rt2 <- initRuntime (NodeAddr "localhost" 0)
  let addr1 = rtNodeId rt1
      addr2 = rtNodeId rt2

  -- connect returns the locally-assigned NodeId for the peer.
  -- NodeId 0 is always self; remotes get ids ≥ 1.
  node2 <- withRuntime rt1 $ connect Nothing addr2
  node1 <- withRuntime rt2 $ connect Nothing addr1

  -- Helper: build a RemoteRef that is visible from one node for an actor
  -- that was spawned on a different node. The actor's own id uses NodeId 0
  -- (self), but from the caller's node it is addressed by the assigned id.
  let toRemote nid ref =
        let ActorId _ u = someActorId (SomeActorRef ref)
        in  RemoteRef (ActorId nid u)

  -- Spawn workers on node 2.
  echoRef  <- withRuntime rt2 $ spawnActor echoActor   stopOnDeath ()
  printRef <- withRuntime rt2 $ spawnActor printerActor stopOnDeath ()

  let remoteEcho  = toRemote node2 echoRef  :: ActorRef String String
      remotePrint = toRemote node2 printRef :: ActorRef String ()

  -- 1. Remote cast: fire-and-forget from node 1 to node 2.
  putStrLn "\n[1] remote cast  node 1 -> node 2"
  withRuntime rt1 $ cast' "hello from node 1" remotePrint
  threadDelay 300_000

  -- 2. Remote call: request/reply across nodes.
  putStrLn "\n[2] remote call  node 1 <-> node 2"
  reply <- withRuntime rt1 $ call' "ping" remoteEcho
  putStrLn $ "    reply: " <> show reply

  -- 3. Reverse cast: actor on node 2 sends to an actor on node 1.
  putStrLn "\n[3] reverse cast  node 2 -> node 1"
  recvVar <- newTVarIO (Nothing :: Maybe String)
  recvRef <- withRuntime rt1 $ spawnActor (recvActorFn recvVar) stopOnDeath ()
  let remoteRecv = toRemote node1 recvRef :: ActorRef String ()
  withRuntime rt2 $ cast' "hello from node 2" remoteRecv
  threadDelay 300_000
  readTVarIO recvVar >>= \v -> putStrLn $ "    received: " <> show v

  -- 4. Cross-node death notification.
  --
  -- The mortal actor lives on node 2.  When it dies, it sends NMDeath to
  -- node 1.  The watcher actor on node 1 has registered interest in the
  -- mortal's ActorId; routeRemoteDeath finds it and delivers the message.
  putStrLn "\n[4] cross-node death notification"
  diedVar  <- newTVarIO False
  watchRef <- withRuntime rt1 $ spawnActor noopActor
    (\dm -> do
        liftIO $ putStrLn $ "  [watcher] death of " <> show (dmActorId dm)
        liftIO $ atomically $ writeTVar diedVar True
        return Stop)
    ()
  mortalRef <- withRuntime rt2 $ spawnActor noopActor stopOnDeath ()

  let mortalId = someActorId (SomeActorRef mortalRef)
      watchId  = someActorId (SomeActorRef watchRef)

  -- mortalRef's links: on death, send NMDeath to node 1's address.
  withRuntime rt2 $
    linkActorTo (RemoteTarget watchId  addr1) mortalRef
  -- watchRef's links: declare interest in mortalId so routeRemoteDeath
  -- on node 1 can find this actor when NMDeath arrives.
  withRuntime rt1 $
    linkActorTo (RemoteTarget mortalId addr2) watchRef

  -- Kill the mortal actor by posting directly to its death queue.
  case mortalRef of
    LocalRef {arDeathQ, arState} ->
      atomically $ writeTQueue arDeathQ (DeathMessage (asId arState) Killed)
    RemoteRef _ -> return ()

  threadDelay 500_000
  readTVarIO diedVar >>= \d -> putStrLn $ "    death delivered: " <> show d

  putStrLn "\n=== done ==="
