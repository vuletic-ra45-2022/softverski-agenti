module FL.Actors.Logger
  ( loggerHandler
  , logFn
  , logFn'
  ) where

import Control.Actor.Core (ActorM, Handler, cast, cast', pass)
import Control.Actor.Runtime (RuntimeM)
import Control.Actor.Types
import Control.Monad.IO.Class (liftIO)
import Data.Time.Clock (getCurrentTime)
import FL.Types

type LogState = ()

loggerHandler :: Handler LogMsg LogState ()
loggerHandler (Log LogEntry{leSource, leLevel, leMsg}) = do
  t <- liftIO getCurrentTime
  liftIO $ putStrLn $
    "[" <> show t <> "] [" <> showLevel leLevel <> "] "
    <> leSource <> ": " <> leMsg
  pass
  where
    showLevel DEBUG = "DEBUG"
    showLevel INFO  = "INFO "
    showLevel WARN  = "WARN "
    showLevel ERROR = "ERROR"

-- | Log via the logger actor. Goes through the normal cast path so it works
-- for both local and remote logger refs (a remote ref used to be a silent
-- no-op, losing all trainer-node logs in provider mode).
logFn :: ActorRef LogMsg () -> String -> LogLevel -> String -> ActorM u ()
logFn ref src lvl msg = cast (Log (LogEntry src lvl msg)) ref

-- | Same as 'logFn' but usable outside an actor (node startup code).
logFn' :: ActorRef LogMsg () -> String -> LogLevel -> String -> RuntimeM ()
logFn' ref src lvl msg = cast' (Log (LogEntry src lvl msg)) ref
