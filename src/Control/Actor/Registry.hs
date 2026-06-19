module Control.Actor.Registry
  ( registerActor,
    lookupActor,
    lookupRemoteActor,
    registryUUID,
    registry,
    registerDeath,
    createRegistry,
  )
where

import Control.Actor.Core (ActorM, ActorResult (..), Handler, call, call', cast', liftRuntime, linkActorTo, pass, spawnActorAs, state, lastMessageFrom, cast, passWith)
import Control.Actor.Runtime (Runtime (..), RuntimeM, getActorByUUID, withRuntime)
import Control.Actor.Types (ActorId (..), ActorRef (..), DeathMessage (..), DeathTarget (LocalTarget), RegistryMsg (..), SomeActorRef (..), SupervisorAction (Continue), actorRefId, thisNodeId, findByUUID)
import Control.Concurrent.STM (readTVarIO)
import Control.Monad (forM_, when)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (ask)
import Data.Map qualified as Map
import Data.UUID (UUID, fromWords)
import Unsafe.Coerce (unsafeCoerce)

registryUUID :: UUID
registryUUID = fromWords 0 0 0 1

type RegistryState = Map.Map String ActorId

registryHandlerFn :: Handler RegistryMsg RegistryState (Maybe ActorId)
registryHandlerFn msg@(RMRegister name uuid) = do
  u <- state
  from <- lastMessageFrom
  when (from == thisNodeId) $ do
    rt <- liftRuntime ask
    nodeTable <- liftIO $ readTVarIO (rtNodeTable rt)
    forM_ (Map.keys nodeTable) $ \nodeId -> do
      cast msg (RemoteRef (ActorId nodeId registryUUID))
  passWith (Map.insert name (ActorId from uuid) u)
registryHandlerFn (RMLookup name) = do
  u <- state
  return $ ActorResult (Just (Map.lookup name u)) u Nothing
registryHandlerFn (RMDeregister name) = do
  u <- state
  return $ ActorResult Nothing (Map.delete name u) Nothing
registryHandlerFn msg@(RMDeath deadUUID) = do
  u <- state
  from <- lastMessageFrom

  let (_, kept) = Map.partition (\(ActorId _ uuid) -> uuid == deadUUID) u

  when (from == thisNodeId) $ do
    rt <- liftRuntime ask
    nodeTable <- liftIO $ readTVarIO (rtNodeTable rt)
    forM_ (Map.keys nodeTable) $ \nodeId -> do
      cast msg (RemoteRef (ActorId nodeId registryUUID))

  passWith kept

registryDeathFn :: DeathMessage -> ActorM RegistryState (SupervisorAction RegistryState)
registryDeathFn (DeathMessage (ActorId _ deadUUID) _) = do
  u <- state
  let (_, kept) = Map.partition (\(ActorId _ uuid) -> uuid == deadUUID) u
  rt <- liftRuntime ask
  nodeTable <- liftIO $ readTVarIO (rtNodeTable rt)
  forM_ (Map.keys nodeTable) $ \nodeId -> do
    cast (RMDeath deadUUID) (RemoteRef (ActorId nodeId registryUUID))

  return $ Continue kept

registry :: RuntimeM (Maybe (ActorRef RegistryMsg (Maybe ActorId)))
registry = do
  unsafeCoerce $ getActorByUUID registryUUID

registerDeath :: ActorId -> RuntimeM ()
registerDeath (ActorId _ uuid) = do
  r <- registry
  mapM_ (cast' (RMDeath uuid)) r

registerActor :: String -> ActorRef msg r -> RuntimeM ()
registerActor name ref = do
  rt <- ask
  let ActorId _ uuid = actorRefId ref
  actors <- liftIO $ readTVarIO (rtActors rt)
  case findByUUID registryUUID actors of
    Just (SomeActorRef regRef) -> do
      let reg = unsafeCoerce regRef :: ActorRef RegistryMsg (Maybe ActorId)
      cast' (RMRegister name uuid) reg
      case reg of
        LocalRef {arDeathQ} -> linkActorTo (LocalTarget arDeathQ) ref
        RemoteRef _ -> return ()
    Nothing -> liftIO $ putStrLn "registerActor: registry not found"

lookupActor :: String -> RuntimeM (Maybe (ActorRef msg r))
lookupActor name = do
  rt <- ask
  actors <- liftIO $ readTVarIO (rtActors rt)
  case findByUUID registryUUID actors of
    Nothing -> return Nothing
    Just (SomeActorRef regRef) -> do
      result <-
        call'
          (RMLookup name)
          (unsafeCoerce regRef :: ActorRef RegistryMsg (Maybe ActorId))
      case result of
        Just (Just (ActorId 0 uuid)) -> do
          actors' <- liftIO $ readTVarIO (rtActors rt)
          return $ fmap (\(SomeActorRef r) -> unsafeCoerce r) (findByUUID uuid actors')
        Just (Just aid) -> return $ Just (RemoteRef aid)
        _else -> return Nothing

lookupRemoteActor :: String -> RuntimeM (Maybe (ActorRef msg r))
lookupRemoteActor name = do
  rt <- ask
  table <- liftIO $ readTVarIO (rtNodeTable rt)
  go (Map.keys table)
  where
    go [] = return Nothing
    go (peerNodeId : rest) = do
      let remoteReg =
            RemoteRef (ActorId peerNodeId registryUUID) ::
              ActorRef RegistryMsg (Maybe ActorId)
      result <- call' (RMLookup name) remoteReg
      case result of
        Just (Just (ActorId 0 uuid)) ->
          return $ Just (RemoteRef (ActorId peerNodeId uuid))
        Just (Just aid) ->
          return $ Just (RemoteRef aid)
        _else -> go rest

createRegistry :: RuntimeM (ActorRef RegistryMsg (Maybe ActorId))
createRegistry =
  spawnActorAs registryUUID registryHandlerFn registryDeathFn Map.empty
