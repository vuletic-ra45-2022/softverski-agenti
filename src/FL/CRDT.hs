module FL.CRDT
  ( GCounter
  , gcEmpty
  , gcIncrement
  , gcMerge
  , gcValue
  , gcGet
  , ORSet
  , orEmpty
  , orAdd
  , orRemove
  , orMerge
  , orMembers
  ) where

import Data.Binary (Binary (..))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set
import Data.UUID (UUID)
import GHC.Generics (Generic)

-- ---------------------------------------------------------------------------
-- G-Counter: each node has its own non-decreasing counter.
-- Merge takes element-wise max. Value is the sum.
-- ---------------------------------------------------------------------------

newtype GCounter = GCounter (Map String Int)
  deriving (Show, Eq, Generic)

instance Binary GCounter where
  put (GCounter m) = put (Map.toAscList m)
  get = GCounter . Map.fromAscList <$> get

gcEmpty :: GCounter
gcEmpty = GCounter Map.empty

gcIncrement :: String -> GCounter -> GCounter
gcIncrement nodeId (GCounter m) =
  GCounter (Map.insertWith (+) nodeId 1 m)

gcMerge :: GCounter -> GCounter -> GCounter
gcMerge (GCounter a) (GCounter b) =
  GCounter (Map.unionWith max a b)

gcValue :: GCounter -> Int
gcValue (GCounter m) = sum (Map.elems m)

gcGet :: String -> GCounter -> Int
gcGet nodeId (GCounter m) = Map.findWithDefault 0 nodeId m

-- ---------------------------------------------------------------------------
-- OR-Set: observe-remove set. Each element tagged with a set of UUIDs.
-- Add: insert a fresh UUID tag. Remove: delete all tags for that element.
-- Merge: union of tag sets per element. An element is present iff it has
-- at least one tag.
-- ---------------------------------------------------------------------------

newtype ORSet a = ORSet (Map a (Set UUID))
  deriving (Show, Eq, Generic)

instance (Ord a, Binary a) => Binary (ORSet a) where
  put (ORSet m) = put (Map.toAscList (fmap Set.toAscList m))
  get = ORSet . Map.fromAscList . map (fmap Set.fromAscList) <$> get

orEmpty :: ORSet a
orEmpty = ORSet Map.empty

orAdd :: Ord a => UUID -> a -> ORSet a -> ORSet a
orAdd tag x (ORSet m) =
  ORSet (Map.insertWith Set.union x (Set.singleton tag) m)

orRemove :: Ord a => a -> ORSet a -> ORSet a
orRemove x (ORSet m) = ORSet (Map.delete x m)

orMerge :: Ord a => ORSet a -> ORSet a -> ORSet a
orMerge (ORSet a) (ORSet b) =
  ORSet (Map.unionWith Set.union a b)

orMembers :: Ord a => ORSet a -> Set a
orMembers (ORSet m) = Map.keysSet (Map.filter (not . Set.null) m)
