module Main where

import           Data.List
import           System.Random
import           Criterion.Main
import           Control.Parallel
import           Control.Parallel.Strategies
import           Control.DeepSeq
import           System.Environment
import qualified Control.Monad.Par             as MP
-- code borrowed from the Stanford Course 240h (Functional Systems in Haskell)
-- I suspect it comes from Bryan O'Sullivan, author of Criterion

data T a = T !a !Int


mean :: (RealFrac a) => [a] -> a
mean = fini . foldl' go (T 0 0)
 where
  fini (T a _) = a
  go (T m n) x = T m' n'
   where
    m' = m + (x - m) / fromIntegral n'
    n' = n + 1


resamples :: Int -> [a] -> [[a]]
resamples k xs =
  take (length xs - k) $ zipWith (++) (inits xs) (map (drop k) (tails xs))


jackknife :: ([a] -> b) -> [a] -> [b]
jackknife f = map f . resamples 500

crud = zipWith (\x a -> sin (x / 300) ** 2 + a) [0 ..]


main = do
  args <- getArgs
  mapM_ putStrLn args
  singelmain (tail args)

singelmain args = do
  let n = read (head args) :: Int
  let (xs, ys) =
        splitAt 1500 (take 6000 (randoms (mkStdGen 211570155)) :: [Float])
  -- handy (later) to give same input different parallel functions
  let rs = crud xs ++ ys
  putStrLn "CMapBuffer"
  --print $ sum $ jackCBuffer mean rs
  --print $ sum $ jackCBuffer' 150 mean rs
  print $ sum $ jackC mean rs
  putStrLn "Done"


--A
jackA :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackA f = amap f . resamples 500

jackADiv :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackADiv n f = divideMap n (amap (map f)) . resamples 500

jackAChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackAChunk n f = chunkMap n (amap (map f)) . resamples 500

-- B

jackB :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackB f = bmap f . resamples 500

jackBDiv :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackBDiv n f = divideMap n (bmap (map f)) . resamples 500

jackBChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackBChunk n f = chunkMap n (bmap (map f)) . resamples 500

--Be

jackBeDiv :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackBeDiv n f = divideMap n (MP.runPar . MP.parMap (map f)) . resamples 500

jackBeChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackBeChunk n f = chunkMap n (MP.runPar . MP.parMap (map f)) . resamples 500

jackBe :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackBe f = MP.runPar . MP.parMap f . resamples 500

-- C
jackC :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackC f = cmap f . resamples 500

jackCDiv :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackCDiv n f = divideMap n (cmap (map f)) . resamples 500

jackCChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackCChunk n f = chunkMap n (cmap (map f)) . resamples 500

-- D
jackD :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackD f = dmap f . resamples 500

jackDDiv :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackDDiv n f = divideMap n (dmap (map f)) . resamples 500

jackDChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackDChunk n f = chunkMap n (dmap (map f)) . resamples 500


amap :: (NFData b) => (a -> b) -> [a] -> [b]
amap f []       = []
amap f (a : as) = par b $ pseq bs (b : bs)
 where
  b  = force $ f a
  bs = amap f as


bmap :: (NFData b) => (a -> b) -> [a] -> [b]
bmap f []       =  []
bmap f (a : as) = runEval $ do
  b  <- rpar $ force (f a)
  let bs = bmap f as
  rseq bs
  rseq b
  return (b : bs)


cmap :: (NFData b) => (a -> b) -> [a] -> [b]
cmap f xs = map f xs `using` parList rdeepseq


dmap :: NFData b => (a -> b) -> [a] -> [b]
dmap f as = MP.runPar $ do
      ibs <- mapM (MP.spawn . return . f) as
      mapM MP.get ibs

chunk :: Int -> [a] -> [[a]]
chunk n [] = []
chunk n xs = as : chunk n bs where (as, bs) = splitAt n xs


chunkMap :: (NFData b) => Int -> ([[a]] -> [[b]]) -> [a] -> [b]
chunkMap n mapF = concat . mapF . chunk n


divide :: Int -> [a] -> [[a]]
divide n xs = chunk d xs
    where d = ceiling (len / fromIntegral n)
          len = fromIntegral $ length xs

divideMap :: (NFData b) => Int -> ([[a]] -> [[b]]) -> [a] -> [b]
divideMap n mapF = concat . mapF . divide n

-- 2 ------------------------------------------------------------

--divConq :: (a -> b) -> a -> (a -> [a]) -> ([b] -> b) -> b
--divConq f arg divide combine  = combine $ map f (devide arg)


divConq :: (NFData b) => (a -> b) -> a -> (a -> Bool) -> (a -> Maybe [a]) -> ([b] -> b) -> b
divConq f arg threshold divide combine  = go arg
    where
        go arg =
            case divide arg of
                Nothing -> f arg
                Just xs -> combine $ mapF go xs

                where mapF | threshold arg = map
                           | otherwise     = dmap



sortL :: (Ord a) => [a] -> [a] -> [a]
sortL [] yl = yl
sortL xl [] = xl
sortL (x:xs) (y:ys) | x <= y = x : sortL xs (y:ys)
                   | otherwise = y : sortL (x:xs) ys

mergeSort :: (Ord a,NFData a) => Int -> [a] -> [a]
mergeSort d xs = divConq f xs threshold divide combine
    where
        f :: Ord a => [a] -> [a]
        f x = x

        threshold :: [a] -> Bool
        threshold x = length x < d

        divide xs = case splitAt (div (length xs) 2) xs of
                        ([],l) -> Nothing
                        (l,[]) -> Nothing
                        (l1,l2) -> Just [l1,l2]

        combine :: Ord a => [[a]] -> [a]
        combine [[], ys] = ys
        combine [xs, []] = xs
        combine [x:xs, y:ys] | x <= y    = x : combine [xs, y:ys]
                             | otherwise = y : combine [x:xs, ys]


-- 3 -----------------------------------------------------------------------------
--cmapbuffer :: (NFData b) => (a -> b) -> [a] -> [b]
--cmapbuffer f xs = map f xs `using` parBuffer 100 rdeepseq

cmapbuffer :: (NFData b) => (a -> b) -> [a] -> [b]
cmapbuffer f = withStrategy (parBuffer 100 rdeepseq) . map f

jackCBuffer :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackCBuffer f = cmapbuffer f . resamples 500

jackCBuffer' :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackCBuffer' n f = chunkMap n (cmap (cmapbuffer f)) . resamples 500