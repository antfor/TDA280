module Main where

import           Data.List
import           System.Random
import           Criterion.Main
import           Control.Parallel
import           Control.Parallel.Strategies
import           Control.DeepSeq
import           System.Environment
import qualified Control.Monad.Par             as MP

-- Code from given.hs -------------------------------------------------------
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

-- Main function and benchmarking -------------------------------------------
main :: IO ()
main = do
  args <- getArgs
  mapM_ putStrLn args
  let fun = head args
  case fun of
    "b1" -> benchmain1 (tail args)
    "b2" -> benchmain2 (tail args)
    _   -> putStrLn "Bad arg"

-- Benchmarks jackknife related functions
benchmain1 :: IO ()
benchmain1 args = do
  let n = read (head args) :: Int

  let (xs, ys) =
        splitAt 1500 (take 6000 (randoms (mkStdGen 211570155)) :: [Float])

  let rs = crud xs ++ ys
  putStrLn $ "sample mean:    " ++ show (mean rs)

  let j = jackknife mean rs :: [Float]
  putStrLn $ "jack mean min:  " ++ show (minimum j)
  putStrLn $ "jack mean max:  " ++ show (maximum j)
  print $ sum rs

  withArgs (drop 1 args) $ defaultMain [
    bench "jackknife" (nf (jackknife mean) rs),
    bench "jackknifeA" (nf (jackA mean) rs),
    bench "jackknifeAChunk" (nf (jackAChunk n mean) rs),
    bench "jackknifeB" (nf (jackB mean) rs),
    bench "jackknifeBChunk" (nf (jackBChunk n mean) rs),
    bench "jackknifeBe" (nf (jackBe mean) rs),
    bench "jackknifeBeChunk" (nf (jackBeChunk n mean) rs),
    bench "jackknifeC" (nf (jackC mean) rs),
    bench "jackknifeCChunk" (nf (jackCChunk n mean) rs),
    bench "jackknifeD" (nf (jackD mean) rs),
    bench "jackknifeDChunk" (nf (jackDChunk n mean) rs)
    ]

-- Benchmarks DivConq related functions
benchmain2 :: IO ()
benchmain2 args = do
  let n = read (head args) :: Int

  let xs = take 200000 (randoms (mkStdGen 211570155)) :: [Int]
  
  let i = 1
  let ys = xs ++ [1]

  withArgs (drop 1 args) $ defaultMain [
    bench "MergeSort" (nf (mergeSort False n) xs),
    bench "MergeSortPar" (nf (mergeSort True n) xs),
    bench "Search" (nf (search False n i) ys),
    bench "SearchPar" (nf (search True n i) ys)
    ]

-- 1 ------------------------------------------------------------------------
-- Parallel jackknife using amap
jackA :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackA f = amap f . resamples 500

-- Parallel jackknife using amap and chunking
jackAChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackAChunk n f = chunkMap n (amap (map f)) . resamples 500

-- Parallel jackknife using bmap
jackB :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackB f = bmap f . resamples 500

-- Parallel jackknife using bmap and chunking
jackBChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackBChunk n f = chunkMap n (bmap (map f)) . resamples 500

-- Parallel jackknife using parMap
jackBe :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackBe f = MP.runPar . MP.parMap f . resamples 500

-- Parallel jackknife using parMap and chunking
jackBeChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackBeChunk n f = chunkMap n (MP.runPar . MP.parMap (map f)) . resamples 500

-- Parallel jackknife using cmap
jackC :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackC f = cmap f . resamples 500

-- Parallel jackknife using cmap and chunking
jackCChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackCChunk n f = chunkMap n (cmap (map f)) . resamples 500

-- Parallel jackknife using dmap
jackD :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackD f = dmap f . resamples 500

-- Parallel jackknife using dmap and chunking
jackDChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackDChunk n f = chunkMap n (dmap (map f)) . resamples 500

-- Parallel map function using par and pseq
amap :: (NFData b) => (a -> b) -> [a] -> [b]
amap f []       = []
amap f (a : as) = par b $ pseq bs (b : bs)
 where
  b  = force $ f a
  bs = amap f as

-- Parallel map function using the eval monad
bmap :: (NFData b) => (a -> b) -> [a] -> [b]
bmap f []       =  []
bmap f (a : as) = runEval $ do
  b  <- rpar $ force (f a)
  let bs = bmap f as
  rseq bs
  rseq b
  return (b : bs)

-- Parallel map function using the strategy parList and rdeepseq
cmap :: (NFData b) => (a -> b) -> [a] -> [b]
cmap f xs = map f xs `using` parList rdeepseq

-- Parallel map function using the par monad
dmap :: NFData b => (a -> b) -> [a] -> [b]
dmap f as = MP.runPar $ do
      ibs <- mapM (MP.spawn . return . f) as
      mapM MP.get ibs

-- Splits a list into a list of smaller lists of length n
chunk :: Int -> [a] -> [[a]]
chunk n [] = []
chunk n xs = as : chunk n bs where (as, bs) = splitAt n xs

-- Given a mapping function, this function maps over a list of chunks and
-- concatenates the result
chunkMap :: (NFData b) => Int -> ([[a]] -> [[b]]) -> [a] -> [b]
chunkMap n mapF = concat . mapF . chunk n

-- 2 ------------------------------------------------------------------------
-- A generic divide and conquer algorithm
--   f:       what to do when it is not possible to divide
--   arg:     input data structure
--   divide:  specifies how to divide the data structure
--   combine: specifies how to combine the divided data structure
divConq :: (a -> b) -> 
           a -> 
           (a -> Maybe [a]) -> 
           ([b] -> b) -> b
divConq f arg divide combine  = go arg
    where
        go arg =
            case divide arg of
                Nothing -> f arg
                Just xs -> combine $ map go xs

-- A generic parallel divide and conquer algorithm. It uses dmap to achieve
-- parallelism. It takes one more argument threshold, when this function
-- returns True map is used instead of dmap
divConqPar :: (NFData b) => 
              (a -> b) -> 
              a -> 
              (a -> Bool) -> 
              (a -> Maybe [a]) -> 
              ([b] -> b) -> b
divConqPar f arg threshold divide combine  = go arg
    where
        go arg =
            case divide arg of
                Nothing -> f arg
                Just xs -> combine $ mapF go xs

                where mapF | threshold arg = map
                           | otherwise     = dmap

-- A merge sort algorithm using the divConq algorithms. The argument usePar
-- determines whether to use the parallel version or not.
mergeSort :: (Ord a,NFData a) => Bool -> Int -> [a] -> [a]
mergeSort usePar d xs = 
  if usePar then divConqPar f xs threshold divide combine
  else divConq f xs divide combine
    where
        f = id

        threshold x = length x < d

        divide xs = case splitAt (div (length xs) 2) xs of
                        ([],l) -> Nothing
                        (l,[]) -> Nothing
                        (l1,l2) -> Just [l1,l2]

        combine [[], ys] = ys
        combine [xs, []] = xs
        combine [x:xs, y:ys] | x <= y    = x : combine [xs, y:ys]
                             | otherwise = y : combine [x:xs, ys]

-- A search algorithm using the divConq algorithms. The argument usePar
-- determines whether to use the parallel version or not.
search :: (Eq a, NFData a) => Bool -> Int -> a -> [a] -> Bool
search usePar d i xs = 
  if usePar then divConqPar f xs threshold divide combine
  else divConq f xs divide combine
    where
        f = elem i

        combine = or

        threshold x = length x < d
        
        divide xs = case splitAt (div (length xs) 2) xs of
                        ([],l) -> Nothing
                        (l,[]) -> Nothing
                        (l1,l2) -> Just [l1,l2]

-- 3 ------------------------------------------------------------------------
-- Parallel map function using the strategy parBuffer and rdeepseq
cmapbuffer :: (NFData b) => (a -> b) -> [a] -> [b]
cmapbuffer f = withStrategy (parBuffer 100 rdeepseq) . map f

-- Parallel jackknife using cmapbuffer
jackCBuffer :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackCBuffer f = cmapbuffer f . resamples 500

-- Parallel jackknife using cmapbuffer and chunking
jackCBuffer' :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackCBuffer' n f = chunkMap n (cmap (cmapbuffer f)) . resamples 500

-- Parallel jackknife using the strategy parListChunk and rdeepseq for its
-- mapping.
jackListChunk :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackListChunk n f = cmapChunk f . resamples 500
    where
        cmapChunk f xs = map f xs `using` parListChunk n rdeepseq