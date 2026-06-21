module Main where

import Control.Actor.Types (NodeAddr (..))
import FL.P2P (runPeer)
import FL.Provider (runCoordinator, runTrainer)
import FL.Types
import Options.Applicative

-- ---------------------------------------------------------------------------
-- CLI
-- ---------------------------------------------------------------------------

data Mode = Provider | P2P deriving (Show, Eq)

data Role = Coordinator | Trainer | Peer deriving (Show, Eq)

data CLIArgs = CLIArgs
  { argMode         :: Mode
  , argRole         :: Role
  , argPort         :: Int
  , argBasePort     :: Int
  , argRounds       :: Int
  , argEpochs       :: Int
  , argLR           :: Double
  , argBatch        :: Int
  , argNumTrainers  :: Int
  , argTrainerId    :: Int
  , argCoordHost    :: String
  , argCoordPort    :: Int
  , argKnownPeers   :: [String]
  , argDataPath     :: Maybe FilePath
  , argPartition    :: PartitionMode
  , argNorm         :: NormMode
  }

parseMode :: String -> Either String Mode
parseMode "provider" = Right Provider
parseMode "p2p"      = Right P2P
parseMode s          = Left ("unknown mode: " <> s)

parsePartition :: String -> Either String PartitionMode
parsePartition "iid"    = Right IID
parsePartition "noniid" = Right NonIID
parsePartition s        = Left ("unknown partition: " <> s)

parseNorm :: String -> Either String NormMode
parseNorm "minmax" = Right MinMax
parseNorm "zscore" = Right ZScore
parseNorm s        = Left ("unknown norm: " <> s)

parseRole :: String -> Either String Role
parseRole "coordinator" = Right Coordinator
parseRole "trainer"     = Right Trainer
parseRole "peer"        = Right Peer
parseRole s             = Left ("unknown role: " <> s)

parsePeer :: String -> NodeAddr
parsePeer s =
  let (host, rest) = break (== ':') s
  in NodeAddr host (read (drop 1 rest))

argsParser :: Parser CLIArgs
argsParser = CLIArgs
  <$> option (eitherReader parseMode)
        (long "mode" <> short 'm' <> metavar "provider|p2p"
         <> help "Topology mode" <> value Provider <> showDefaultWith show)
  <*> option (eitherReader parseRole)
        (long "role" <> short 'r' <> metavar "coordinator|trainer|peer"
         <> help "Node role" <> value Coordinator <> showDefaultWith show)
  <*> option auto
        (long "port" <> metavar "PORT"
         <> help "This node's port" <> value 9100 <> showDefault)
  <*> option auto
        (long "base-port" <> metavar "PORT"
         <> help "Base port (coordinator uses this)" <> value 9100 <> showDefault)
  <*> option auto
        (long "rounds" <> metavar "N"
         <> help "Number of federated rounds" <> value 10 <> showDefault)
  <*> option auto
        (long "epochs" <> short 'e' <> metavar "N"
         <> help "Local training epochs per round" <> value 5 <> showDefault)
  <*> option auto
        (long "lr" <> metavar "LR"
         <> help "Learning rate" <> value 0.01 <> showDefault)
  <*> option auto
        (long "batch" <> metavar "N"
         <> help "Mini-batch size" <> value 32 <> showDefault)
  <*> option auto
        (long "trainers" <> short 't' <> metavar "N"
         <> help "Expected number of trainers/peers" <> value 3 <> showDefault)
  <*> option auto
        (long "id" <> metavar "N"
         <> help "Trainer/peer index (0-based)" <> value 0 <> showDefault)
  <*> strOption
        (long "coord-host" <> metavar "HOST"
         <> help "Coordinator hostname (trainer mode)" <> value "localhost")
  <*> option auto
        (long "coord-port" <> metavar "PORT"
         <> help "Coordinator port (trainer mode)" <> value 9100 <> showDefault)
  <*> many (strOption
        (long "peer" <> metavar "HOST:PORT"
         <> help "Known peer address (p2p mode, repeat for each peer)"))
  <*> optional (strOption
        (long "data" <> short 'd' <> metavar "PATH"
         <> help "Path to PaySim CSV (uses synthetic data if absent)"))
  <*> option (eitherReader parsePartition)
        (long "partition" <> metavar "iid|noniid"
         <> help "Data partition mode" <> value IID <> showDefaultWith show)
  <*> option (eitherReader parseNorm)
        (long "norm" <> metavar "minmax|zscore"
         <> help "Feature normalization mode" <> value MinMax <> showDefaultWith show)

main :: IO ()
main = do
  args <- execParser $
    info (argsParser <**> helper)
      ( fullDesc
        <> progDesc "Federated Learning on the Haskell Actor Framework"
        <> header "fl-actors — FedAvg fraud detection"
      )

  let cfg = FLConfig
        { cfgRounds       = argRounds args
        , cfgEpochs       = argEpochs args
        , cfgLearningRate = argLR args
        , cfgBatchSize    = argBatch args
        , cfgNumTrainers  = argNumTrainers args
        , cfgDataPath     = argDataPath args
        , cfgPartition    = argPartition args
        , cfgNorm         = argNorm args
        , cfgBasePort     = argBasePort args
        }

  case (argMode args, argRole args) of
    (Provider, Coordinator) ->
      runCoordinator cfg { cfgBasePort = argPort args }

    (Provider, Trainer) -> do
      let coordAddr = NodeAddr (argCoordHost args) (fromIntegral (argCoordPort args))
      runTrainer cfg (argTrainerId args) coordAddr

    (P2P, Peer) -> do
      let peers = map parsePeer (argKnownPeers args)
          cfg'  = cfg { cfgBasePort = argPort args }
      runPeer cfg' (argTrainerId args) peers

    (mode, role) ->
      putStrLn $ "Invalid combination: mode=" <> show mode <> " role=" <> show role
        <> "\n  provider mode uses: --role=coordinator or --role=trainer"
        <> "\n  p2p mode uses: --role=peer"
