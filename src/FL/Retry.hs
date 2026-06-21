-- | Startup helpers shared by the provider and P2P entry points: retry a
-- node connection or a registry lookup until it succeeds or attempts run out.
module FL.Retry
  ( retryConnect
  , retryLookup
  ) where

import Control.Actor (ActorRef, NodeAddr, RuntimeM, connect, withRuntime)
import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Reader (ask)

retryConnect :: Int -> NodeAddr -> RuntimeM ()
retryConnect 0 addr = error $ "retryConnect: could not connect to " <> show addr
retryConnect n addr = do
  rt     <- ask
  result <- liftIO $ try @SomeException $ withRuntime rt $ void (connect Nothing addr)
  case result of
    Right () -> return ()
    Left  _  -> do
      liftIO $ threadDelay 1_000_000
      retryConnect (n - 1) addr

retryLookup :: Int -> RuntimeM (Maybe (ActorRef msg r)) -> RuntimeM (ActorRef msg r)
retryLookup 0 _ = error "retryLookup: exhausted retries"
retryLookup n action = do
  result <- action
  case result of
    Just ref -> return ref
    Nothing  -> do
      liftIO $ threadDelay 500_000
      retryLookup (n - 1) action
