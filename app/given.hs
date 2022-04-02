module Main where

import Data.List
import System.Random
import Criterion.Main
import Control.Parallel
import Control.Parallel.Strategies
import Control.DeepSeq
import System.Environment
import qualified Control.Monad.Par as MP
-- code borrowed from the Stanford Course 240h (Functional Systems in Haskell)
-- I suspect it comes from Bryan O'Sullivan, author of Criterion

data T a = T !a !Int


mean :: (RealFrac a) => [a] -> a
mean = fini . foldl' go (T 0 0)
  where
    fini (T a _) = a
    go (T m n) x = T m' n'
      where m' = m + (x - m) / fromIntegral n'
            n' = n + 1


resamples :: Int -> [a] -> [[a]]
resamples k xs =
    take (length xs - k) $
    zipWith (++) (inits xs) (map (drop k) (tails xs))


jackknife :: ([a] -> b) -> [a] -> [b]
jackknife f = map f . resamples 500

crud = zipWith (\x a -> sin (x / 300)**2 + a) [0..]

main = do
  args <- getArgs
  mapM_ putStrLn args
  let n = read (head args) :: Int

  let (xs,ys) = splitAt 1500  (take 6000
                               (randoms (mkStdGen 211570155)) :: [Float] )
  -- handy (later) to give same input different parallel functions

  let rs = crud xs ++ ys
  putStrLn $ "sample mean:    " ++ show (mean rs)

  let j = jackknife mean rs :: [Float]
  putStrLn $ "jack mean min:  " ++ show (minimum j)
  putStrLn $ "jack mean max:  " ++ show (maximum j)
  print $ sum rs
  print n
 -- print $ jackknife mean rs == jackB mean rs
  withArgs (drop 1 args) $ defaultMain
        [
-- {-
    --      bench "jackknife" (nf (jackknife mean) rs),
        --  bench "A" (nf (jackA mean) rs)
    --      bench "B" (nf (jackB n mean) rs),
          -- bench "B" (nf (jackB n mean) rs)
          --bench "A'" (nf (jackA2 n mean) rs)
           bench "A'" (nf (jackA2 n mean) rs),
    --      bench "A" (nf (jackA mean) rs)
    --       bench "B" (nf (jackB n mean) rs)
    --     bench "Be2" (nf (jackBe2 mean) rs),
    --     bench "Be" (nf (jackBe n mean) rs)
 -- -}     
            bench "C'" (nf (jackCparmap' mean) rs)
            --bench "C" (nf (jackCparmap mean) rs),
            --bench "jackC" (nf (jackC mean) rs)
         ]




jackA :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackA f = amap f . resamples 500

jackA' ::(NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackA' n f =  divideMap n (amap (map f)) . resamples 500

jackA2 ::(NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackA2 n f =  chunkMap n (amap (map f)) . resamples 500

jackB :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackB n f = divideMap n (runEval . bmap (map f)) . resamples 500

jackBe :: (NFData b) => Int -> ([a] -> b) -> [a] -> [b]
jackBe n f = divideMap n (MP.runPar . MP.parMap (map f)) . resamples 500

jackBe2 :: (NFData b) =>  ([a] -> b) -> [a] -> [b]
jackBe2 f = MP.runPar . MP.parMap f . resamples 500

jackC :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackC f = cmap f . resamples 500

jackCparmap :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackCparmap f = cparmap f . resamples 500

jackCparmap' :: (NFData b) => ([a] -> b) -> [a] -> [b]
jackCparmap' f = cparmap' f . resamples 500


amap :: (NFData b) => (a -> b) -> [a] -> [b] 
amap f []     = []
amap f (a:as) = par b $ pseq bs (b:bs)
 where
     b  = force $ f a
     bs = amap f as


bmap :: (NFData b) => (a -> b) -> [a] -> Eval [b]
bmap f []     = return []
bmap f (a:as) = do
              b   <- rpar $ force (f a)
              bs  <- bmap f as
              rseq bs
              rseq b
              return (b:bs)

cmap :: (NFData b) => (a -> b) -> [a] -> [b]
cmap f []     = []
cmap f (a:as) = (b:bs) `using` strat
    where
        b = force (f a)
        bs = cmap f as
        strat v = do rpar b; rseq bs; return v


cparmap ::  (a -> b) -> [a] -> [b]
cparmap f xs = map f xs `using` parList rseq

cparmap' :: (NFData b) => (a -> b) -> [a] -> [b]
cparmap' f xs = map f xs `using` parList rdeepseq

chunk :: Int -> [a] -> [[a]]
chunk n [] = []
chunk n xs = as : chunk n bs
    where
        (as,bs) = splitAt n xs

divide :: Int -> [a] -> [[a]]
divide n xs = chunk d xs
    where d = ceiling (len / fromIntegral n)
          len = fromIntegral $ length xs

divideMap :: (NFData b) => Int -> ([[a]] -> [[b]]) -> [a] -> [b]
divideMap n mapF = concat . mapF . divide n

chunkMap :: (NFData b) => Int -> ([[a]] -> [[b]]) -> [a] -> [b]
chunkMap n mapF = concat . mapF . chunk n
