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
  ) where

import Control.Actor.Core
import Control.Actor.Network
import Control.Actor.Runtime
import Control.Actor.Supervision
import Control.Actor.Transport
import Control.Actor.Types

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
  ( TMVar, atomically, newEmptyTMVarIO, readTMVar )
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
