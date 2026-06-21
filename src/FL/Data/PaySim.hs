{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module FL.Data.PaySim
  ( loadAndPartition
  -- , syntheticPartition
  ) where

import Data.ByteString.Lazy qualified as BL
import Data.Csv qualified as Csv
import Data.List (sortBy)
import Data.Ord (comparing)
import Data.Vector qualified as V
import FL.NN (LocalData (..))
import FL.Types (FLConfig (..), NormMode (..), PartitionMode (..))
import GHC.Generics (Generic)
import Numeric.LinearAlgebra hiding ((<>))
import Numeric.LinearAlgebra qualified as LA
import System.Random (mkStdGen, randomRs)

-- ---------------------------------------------------------------------------
-- PaySim CSV record
-- ---------------------------------------------------------------------------

data PaySimRow = PaySimRow
  { rowStep           :: !Double
  , rowType           :: !String
  , rowAmount         :: !Double
  , rowOldBalOrig     :: !Double
  , rowNewBalOrig     :: !Double
  , rowOldBalDest     :: !Double
  , rowNewBalDest     :: !Double
  , rowIsFraud        :: !Double
  } deriving (Show, Generic)

instance Csv.FromNamedRecord PaySimRow where
  parseNamedRecord r =
    PaySimRow
      <$> r Csv..: "step"
      <*> r Csv..: "type"
      <*> r Csv..: "amount"
      <*> r Csv..: "oldbalanceOrg"
      <*> r Csv..: "newbalanceOrig"
      <*> r Csv..: "oldbalanceDest"
      <*> r Csv..: "newbalanceDest"
      <*> r Csv..: "isFraud"

rowToFeatures :: PaySimRow -> Vector Double
rowToFeatures PaySimRow{..} =
  let typeVec = case rowType of
        "CASH_IN"   -> [1,0,0,0,0]
        "CASH_OUT"  -> [0,1,0,0,0]
        "DEBIT"     -> [0,0,1,0,0]
        "PAYMENT"   -> [0,0,0,1,0]
        "TRANSFER"  -> [0,0,0,0,1]
        _           -> [0,0,0,0,0]
      numeric = [ rowAmount
                , rowOldBalOrig
                , rowNewBalOrig
                , rowOldBalDest
                , rowNewBalDest
                , rowNewBalOrig - rowOldBalOrig   -- balance change orig
                , rowNewBalDest - rowOldBalDest   -- balance change dest
                , rowAmount / (rowOldBalOrig + 1) -- amount ratio orig
                , rowAmount / (rowOldBalDest + 1) -- amount ratio dest
                , rowStep / 744                   -- normalized step (max 744 in PaySim)
                ]
      -- pad to 29 features (5 one-hot + 10 numeric + 14 zeros)
      padded = typeVec ++ numeric ++ replicate 14 0.0
  in fromList padded

-- ---------------------------------------------------------------------------
-- Load PaySim CSV
-- ---------------------------------------------------------------------------

loadPaySimCSV :: FilePath -> IO (Either String (Matrix Double, Vector Double))
loadPaySimCSV path = do
  raw <- BL.readFile path
  case Csv.decodeByName raw of
    Left err -> return (Left err)
    Right (_, rows) -> do
      let featureList = V.toList (V.map rowToFeatures rows)
          labelList   = V.toList (V.map rowIsFraud rows)
      return (Right (fromRows featureList, fromList labelList))

-- ---------------------------------------------------------------------------
-- Normalization
-- ---------------------------------------------------------------------------

normalizeMinMax :: Matrix Double -> (Matrix Double, Vector Double, Vector Double)
normalizeMinMax m =
  let mins  = fromList [minElement (m ¿ [j]) | j <- [0 .. cols m - 1]]
      maxs  = fromList [maxElement (m ¿ [j]) | j <- [0 .. cols m - 1]]
      range = cmap (\x -> if x == 0 then 1 else x) (maxs - mins)
      norm  = (m - LA.repmat (asRow mins) (rows m) 1)
              / LA.repmat (asRow range)  (rows m) 1
  in (norm, mins, maxs)

normalizeZScore :: Matrix Double -> (Matrix Double, Vector Double, Vector Double)
normalizeZScore m =
  let n     = fromIntegral (rows m) :: Double
      means = fromList [sumElements (m ¿ [j]) / n | j <- [0 .. cols m - 1]]
      vars  = fromList [sumElements (cmap (^(2::Int)) ((m ¿ [j]) - scalar (means ! j))) / n
                       | j <- [0 .. cols m - 1]]
      stds  = cmap (\v -> if v == 0 then 1 else sqrt v) vars
      norm  = (m - LA.repmat (asRow means) (rows m) 1)
              / LA.repmat (asRow stds)  (rows m) 1
  in (norm, means, stds)

applyNorm :: NormMode -> (Matrix Double, Vector Double, Vector Double) -> Matrix Double -> Matrix Double
applyNorm mode (_, p1, p2) m =
  case mode of
    MinMax ->
      let range = cmap (\x -> if x == 0 then 1 else x) (p2 - p1)
      in (m - LA.repmat (asRow p1) (rows m) 1) / LA.repmat (asRow range) (rows m) 1
    ZScore ->
      let stds = cmap (\x -> if x == 0 then 1 else x) p2
      in (m - LA.repmat (asRow p1) (rows m) 1) / LA.repmat (asRow stds)  (rows m) 1

-- ---------------------------------------------------------------------------
-- IID partition: deterministic shard by trainer index
-- ---------------------------------------------------------------------------

partitionIID :: Int -> Int -> Matrix Double -> Vector Double -> LocalData
partitionIID trainerIdx numTrainers feats labels =
  let n       = rows feats
      -- Shuffle with fixed seed for reproducibility across processes
      seed    = 2024
      rng     = mkStdGen seed
      sorted  = sortBy (comparing fst)
                  (zip (randomRs (0.0 :: Double, 1.0) rng) [0 .. n - 1])
      idxs    = map snd sorted
      shard   = chunkedShard trainerIdx numTrainers idxs
      f       = feats ? shard
      l       = fromList [labels ! i | i <- shard]
  in LocalData f l

chunkedShard :: Int -> Int -> [a] -> [a]
chunkedShard idx total xs =
  let n       = length xs
      size    = n `div` total
      start   = idx * size
      end     = if idx == total - 1 then n else start + size
  in take (end - start) (drop start xs)

-- ---------------------------------------------------------------------------
-- Non-IID partition: each trainer gets dominant transaction type
-- (approximated via deterministic row assignment)
-- ---------------------------------------------------------------------------

partitionNonIID :: Int -> Int -> Matrix Double -> Vector Double -> LocalData
partitionNonIID trainerIdx numTrainers feats labels =
  let n         = rows feats
      -- Assign rows to trainers based on dominant type one-hot (cols 0–4)
      -- Trainer i "owns" type (i mod 5); remaining trainers share type 0
      ownType   = trainerIdx `mod` 5
      isOwn i   = let row = toList (flatten (feats ? [i]))
                  in length row > ownType && row !! ownType > 0.5
      -- With more than 5 trainers several trainers own the same type;
      -- split that type's rows among them instead of giving each trainer
      -- all of them (which duplicated data across shards).
      sharers   = [i | i <- [0 .. numTrainers - 1], i `mod` 5 == ownType]
      shareIdx  = length (takeWhile (/= trainerIdx) sharers)
      -- Gather own-type rows + 10% of other rows for minority
      ownIdxs   = chunkedShard shareIdx (length sharers) [i | i <- [0 .. n-1], isOwn i]
      otherIdxs = [i | i <- [0 .. n-1], not (isOwn i)]
      minority  = chunkedShard trainerIdx numTrainers otherIdxs
      allIdxs   = ownIdxs ++ minority
      f         = feats ? allIdxs
      l         = fromList [labels ! i | i <- allIdxs]
  in LocalData f l

-- ---------------------------------------------------------------------------
-- Synthetic data fallback
-- ---------------------------------------------------------------------------

syntheticData :: Int -> Int -> Int -> LocalData
syntheticData trainerIdx numTrainers nSamples =
  let seed    = trainerIdx * 13337 + numTrainers
      rng     = mkStdGen seed
      -- 29 features from normal distribution
      featVals = take (nSamples * 29) (randomRs (-3.0 :: Double, 3.0) rng)
      feats   = (nSamples >< 29) featVals
      -- Base 2% fraud rate, +1% per trainer index for variety (real PaySim
      -- is ~0.1%, but synthetic runs need enough positives to learn from)
      fraudRate = 0.02 + fromIntegral trainerIdx * 0.01
      rng2    = mkStdGen (seed + 1)
      labelVals = take nSamples (randomRs (0.0 :: Double, 1.0) rng2)
      labels  = fromList [if v < fraudRate then 1.0 else 0.0 | v <- labelVals]
  in LocalData feats labels

-- ---------------------------------------------------------------------------
-- Top-level entry point used by Provider and P2P startup
-- ---------------------------------------------------------------------------

-- Returns (train partition, test data).
-- Test data is the first 10% of the full dataset (at least 1000 rows),
-- taken before partitioning so every node evaluates on the same split.
loadAndPartition
  :: FLConfig
  -> Int    -- this node's trainer index (0-based)
  -> IO (LocalData, LocalData)
loadAndPartition FLConfig{..} trainerIdx = do
  result <- case cfgDataPath of
    Nothing   -> return (Left "no data path")
    Just path -> loadPaySimCSV path
  case result of
    Left _ -> do
      -- Synthetic fallback
      let testData  = syntheticData (cfgNumTrainers + 1) cfgNumTrainers 2000
          trainData = syntheticData trainerIdx cfgNumTrainers 10000
      return (trainData, testData)
    Right (feats, labels) -> do
      let n         = rows feats
          -- Aim for 1000 test rows for stable metrics, but never take more
          -- than half the data — max alone made testN exceed n on small
          -- CSVs and crashed the train subMatrix with a negative size.
          testN     = min (max 1000 (n `div` 10)) (n `div` 2)
          testFeats = subMatrix (0, 0) (testN, cols feats) feats
          testLabs  = subVector 0 testN labels
          trainF    = subMatrix (testN, 0) (n - testN, cols feats) feats
          trainL    = subVector testN (n - testN) labels
          (normF, p1, p2) = case cfgNorm of
            MinMax -> normalizeMinMax trainF
            ZScore -> normalizeZScore trainF
          normTest  = applyNorm cfgNorm (normF, p1, p2) testFeats
          trainData = case cfgPartition of
            IID    -> partitionIID    trainerIdx cfgNumTrainers normF trainL
            NonIID -> partitionNonIID trainerIdx cfgNumTrainers normF trainL
          testData  = LocalData normTest testLabs
      return (trainData, testData)
