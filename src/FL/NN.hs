{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}

module FL.NN
  ( MLPWeights (..)
  , LocalData (..)
  , initWeights
  , trainLocal
  , evaluate
  , flattenWeights
  , unflattenWeights
  , fedAvg
  , localDataSize
  ) where

import Data.List (foldl', sortBy)
import Data.Ord (Down (..), comparing)
import FL.Types (FLConfig (..), Metrics (..))
import Numeric.LinearAlgebra hiding ((<>))
import Numeric.LinearAlgebra qualified as LA
import System.Random (mkStdGen, randomRs)

-- ---------------------------------------------------------------------------
-- Types
-- ---------------------------------------------------------------------------

data MLPWeights = MLPWeights
  { w1 :: Matrix Double  -- 64 × 29
  , b1 :: Vector Double  -- 64
  , w2 :: Matrix Double  -- 32 × 64
  , b2 :: Vector Double  -- 32
  , w3 :: Matrix Double  --  2 × 32
  , b3 :: Vector Double  --  2
  }

data LocalData = LocalData
  { ldFeatures :: Matrix Double  -- N × 29
  , ldLabels   :: Vector Double  -- N  (0.0 or 1.0)
  }

localDataSize :: LocalData -> Int
localDataSize = rows . ldFeatures

-- ---------------------------------------------------------------------------
-- Weight initialization (Xavier uniform)
-- ---------------------------------------------------------------------------

xavierMatrix :: Int -> Int -> Int -> Matrix Double
xavierMatrix seed fanIn fanOut =
  let bound = sqrt (6.0 / fromIntegral (fanIn + fanOut))
      n     = fanIn * fanOut
      vals  = take n (randomRs (-bound, bound) (mkStdGen seed))
  in (fanOut >< fanIn) vals

initWeights :: IO MLPWeights
initWeights = do
  -- Use fixed seeds so multiple processes with same call order get different
  -- weight initializations only if we pass different seeds; for now deterministic.
  let w1' = xavierMatrix 42  29 64
      b1' = konst 0 64
      w2' = xavierMatrix 137 64 32
      b2' = konst 0 32
      w3' = xavierMatrix 999 32  2
      b3' = konst 0 2
  return MLPWeights{ w1=w1', b1=b1', w2=w2', b2=b2', w3=w3', b3=b3' }

-- ---------------------------------------------------------------------------
-- Activation functions
-- ---------------------------------------------------------------------------

relu :: Matrix Double -> Matrix Double
relu = cmap (max 0)

reluGrad :: Matrix Double -> Matrix Double
reluGrad = cmap (\x -> if x > 0 then 1 else 0)

softmaxRows :: Matrix Double -> Matrix Double
softmaxRows m = fromRows (map softmaxVec (toRows m))
  where
    -- Subtract the row max before exponentiating so large logits
    -- can't overflow exp and poison the weights with NaN.
    softmaxVec v =
      let mx = maxElement v
          e  = cmap (\x -> exp (x - mx)) v
          s  = sumElements e
      in cmap (/ s) e

-- ---------------------------------------------------------------------------
-- Forward pass
-- ---------------------------------------------------------------------------

-- Returns (output probs N×2, z1, a1, z2, a2) for backprop.
forwardWithCache
  :: MLPWeights
  -> Matrix Double  -- N × 29
  -> (Matrix Double, Matrix Double, Matrix Double, Matrix Double, Matrix Double)
forwardWithCache MLPWeights{..} x =
  let z1  = x LA.<> tr w1 + repRows (rows x) b1
      a1  = relu z1
      z2  = a1 LA.<> tr w2 + repRows (rows x) b2
      a2  = relu z2
      z3  = a2 LA.<> tr w3 + repRows (rows x) b3
      out = softmaxRows z3
  in (out, z1, a1, z2, a2)

repRows :: Int -> Vector Double -> Matrix Double
repRows n v = fromRows (replicate n v)

colSums :: Matrix Double -> Vector Double
colSums m = fromList [sumElements (m ¿ [j]) | j <- [0 .. cols m - 1]]

-- ---------------------------------------------------------------------------
-- Loss
-- ---------------------------------------------------------------------------

crossEntropyLoss :: Matrix Double -> Vector Double -> Double
crossEntropyLoss probs labels =
  let n = fromIntegral (rows probs)
      logProbs = cmap (log . max 1e-15) probs
      oneHot   = buildOneHot (rows probs) (map round (toList labels))
      loss     = negate (sumElements (oneHot * logProbs)) / n
  in loss

buildOneHot :: Int -> [Int] -> Matrix Double
buildOneHot n ys =
  fromRows [fromList [if i == y then 1.0 else 0.0 | i <- [0, 1]] | y <- ys]

-- ---------------------------------------------------------------------------
-- Backward pass (mini-batch SGD)
-- ---------------------------------------------------------------------------

stepBatch
  :: MLPWeights
  -> Matrix Double  -- batch features N × 29
  -> Vector Double  -- batch labels N
  -> Double         -- learning rate
  -> (MLPWeights, Double)
stepBatch ws@MLPWeights{..} x labels lr =
  let n              = fromIntegral (rows x) :: Double
      (out, z1, a1, z2, a2) = forwardWithCache ws x
      oneHot         = buildOneHot (rows x) (map round (toList labels))
      -- Output delta: dL/dZ3 = (probs - oneHot) / N
      dZ3            = cmap (/ n) (out - oneHot)           -- N × 2
      dW3            = tr dZ3 LA.<> a2                     -- 2 × 32
      db3            = colSums dZ3                         -- 2
      dA2            = dZ3 LA.<> w3                        -- N × 32
      dZ2            = dA2 * reluGrad z2                   -- N × 32
      dW2            = tr dZ2 LA.<> a1                     -- 32 × 64
      db2            = colSums dZ2                         -- 32
      dA1            = dZ2 LA.<> w2                        -- N × 64
      dZ1            = dA1 * reluGrad z1                   -- N × 64
      dW1            = tr dZ1 LA.<> x                      -- 64 × 29
      db1            = colSums dZ1                         -- 64
      ws'            = MLPWeights
        { w1 = w1 - cmap (* lr) dW1
        , b1 = b1 - cmap (* lr) db1
        , w2 = w2 - cmap (* lr) dW2
        , b2 = b2 - cmap (* lr) db2
        , w3 = w3 - cmap (* lr) dW3
        , b3 = b3 - cmap (* lr) db3
        }
      loss = crossEntropyLoss out labels
  in (ws', loss)

-- ---------------------------------------------------------------------------
-- Training
-- ---------------------------------------------------------------------------

-- Shuffle row indices with a seed, then split into mini-batches.
shuffleIndices :: Int -> Int -> [Int]
shuffleIndices seed n =
  let rng = mkStdGen seed
      tagged = zip (randomRs (0.0 :: Double, 1.0) rng) [0 .. n - 1]
  in map snd (sortBy (comparing fst) tagged)

trainEpoch
  :: MLPWeights
  -> Matrix Double  -- features N × 29
  -> Vector Double  -- labels N
  -> Int            -- batch size
  -> Double         -- learning rate
  -> Int            -- seed for shuffle
  -> (MLPWeights, Double)
trainEpoch ws feats labels batchSize lr seed =
  let n       = rows feats
      idxs    = shuffleIndices seed n
      batches = chunksOf batchSize idxs
      step (w, totalLoss) batch =
        let bFeats  = feats ? batch
            bLabels = fromList [labels ! i | i <- batch]
            (w', l) = stepBatch w bFeats bLabels lr
        in (w', totalLoss + l)
      (wFinal, lossSum) = foldl' step (ws, 0.0) batches
  in (wFinal, lossSum / fromIntegral (length batches))

chunksOf :: Int -> [a] -> [[a]]
chunksOf _ [] = []
chunksOf n xs = let (h, t) = splitAt n xs in h : chunksOf n t

-- | Train for the configured number of epochs. The shuffle seed is derived
-- from the trainer id and round so batches differ across rounds and trainers
-- (a bare epoch counter would replay the identical shuffle every round).
trainLocal :: FLConfig -> String -> Int -> MLPWeights -> LocalData -> IO MLPWeights
trainLocal FLConfig{..} trainerId roundNum initW LocalData{..} = do
  let seedBase = foldl' (\h c -> 31 * h + fromEnum c) (roundNum * 7919) trainerId
      go ws epoch =
        let (ws', _loss) = trainEpoch ws ldFeatures ldLabels cfgBatchSize cfgLearningRate (seedBase + epoch)
        in if epoch >= cfgEpochs then ws'
           else go ws' (epoch + 1)
  return (go initW 1)

-- ---------------------------------------------------------------------------
-- Evaluation
-- ---------------------------------------------------------------------------

evaluate :: MLPWeights -> LocalData -> Int -> Metrics
evaluate ws LocalData{..} round_ =
  let n         = rows ldFeatures
      (probs, _, _, _, _) = forwardWithCache ws ldFeatures
      predLabels = fromList [if (probs ! i ! 1) >= 0.5 then 1.0 else 0.0 | i <- [0 .. n - 1]]
      scores     = fromList [probs ! i ! 1 | i <- [0 .. n - 1]]
      tp = sumElements (predLabels * ldLabels)
      fp = sumElements (predLabels * (1 - ldLabels))
      fn = sumElements ((1 - predLabels) * ldLabels)
      tn = sumElements ((1 - predLabels) * (1 - ldLabels))
      acc  = (tp + tn) / fromIntegral n
      prec = if tp + fp == 0 then 0 else tp / (tp + fp)
      rec  = if tp + fn == 0 then 0 else tp / (tp + fn)
      f1   = if prec + rec == 0 then 0 else 2 * prec * rec / (prec + rec)
      auc  = computeAUCROC (toList scores) (toList ldLabels)
  in Metrics acc prec rec f1 auc round_

computeAUCROC :: [Double] -> [Double] -> Double
computeAUCROC scores labels =
  let nPos = fromIntegral (length (filter (== 1) labels))
      nNeg = fromIntegral (length (filter (== 0) labels))
  in if nPos == 0 || nNeg == 0
     then 0.5
     else
       let sorted = sortBy (comparing (Down . fst)) (zip scores labels)
           points = scanl (\(tp, fp) (_, y) -> (tp + y, fp + (1 - y))) (0, 0) sorted
           trapz (tp0, fp0) (tp1, fp1) =
             let fpr0 = fp0 / nNeg; tpr0 = tp0 / nPos
                 fpr1 = fp1 / nNeg; tpr1 = tp1 / nPos
             in (fpr1 - fpr0) * (tpr0 + tpr1) / 2.0
       in sum (zipWith trapz points (tail points))

-- ---------------------------------------------------------------------------
-- Serialization bridge
-- ---------------------------------------------------------------------------

flattenWeights :: MLPWeights -> [Double]
flattenWeights MLPWeights{..} =
  toList (flatten w1)
  ++ toList b1
  ++ toList (flatten w2)
  ++ toList b2
  ++ toList (flatten w3)
  ++ toList b3

-- Fixed architecture: 29→64→32→2
-- w1: 64*29=1856, b1: 64, w2: 32*64=2048, b2: 32, w3: 2*32=64, b3: 2
unflattenWeights :: [Double] -> MLPWeights
unflattenWeights xs =
  let (xw1, r1) = splitAt (64*29) xs
      (xb1, r2) = splitAt 64     r1
      (xw2, r3) = splitAt (32*64) r2
      (xb2, r4) = splitAt 32     r3
      (xw3, r5) = splitAt (2*32)  r4
      xb3        = take 2 r5
  in MLPWeights
       { w1 = (64 >< 29) xw1
       , b1 = fromList xb1
       , w2 = (32 >< 64) xw2
       , b2 = fromList xb2
       , w3 = ( 2 >< 32) xw3
       , b3 = fromList xb3
       }

-- ---------------------------------------------------------------------------
-- FedAvg
-- ---------------------------------------------------------------------------

-- Weighted average of flat weight vectors.
fedAvg :: [(Int, [Double])] -> [Double]
fedAvg [] = []
fedAvg updates =
  let totalN   = fromIntegral (sum (map fst updates)) :: Double
      weighted (n, ws) = map (* (fromIntegral n / totalN)) ws
  in foldl1 (zipWith (+)) (map weighted updates)
