-- We represent a spin as a single byte.  In principle, we need only
-- two values (-1 or 1), but Futhark represents booleans a a full byte
-- entirely, so using an i8 instead takes no more space, and makes the
-- arithmetic simpler.
type spin = i8

import "lib/github.com/diku-dk/cpprandom/random"

-- Pick an RNG engine and define random distributions for specific types.
module rng_engine = minstd_rand
module rand_f32 = uniform_real_distribution f32 rng_engine
module rand_i8 = uniform_int_distribution i8 rng_engine

-- We can create an few RNG state with 'rng_engine.rng_from_seed [x]',
-- where 'x' is some seed.  We can split one RNG state into many with
-- 'rng_engine.split_rng'.
--
-- For an RNG state 'r', we can generate random integers that are
-- either 0 or 1 by calling 'rand_i8.rand (0i8, 1i8) r'.
--
-- For an RNG state 'r', we can generate random floats in the range
-- (0,1) by calling 'rand_f32.rand (0f32, 1f32) r'.
--
-- Remember to consult
-- https://futhark-lang.org/pkgs/github.com/diku-dk/cpprandom/latest/

def rand = rand_f32.rand (0f32, 1f32)

def rand_spin (rnge:rng_engine.rng) = 
      let (new_rng, r) = rand_i8.rand (0i8, 1i8) rnge
      in (new_rng, r*2-1)

-- Create a new grid of a given size.  Also produce an identically
-- sized array of RNG states.
def random_grid (seed: i32) (h: i64) (w: i64)
              : ([h][w]rng_engine.rng, [h][w]spin) =
        let rng = rng_engine.rng_from_seed [seed]
        let len = h*w
        let rngs = rng_engine.split_rng len rng
        let flat_grid = map rand_spin rngs 
        let (l1,l2) = unzip flat_grid 
        in (unflatten h w l1, unflatten h w l2)

-- Compute $\Delta_e$ for each spin in the grid, using wraparound at
-- the edges.
def deltas [h][w] (spins: [h][w]spin): [h][w]i8 =
            let c = spins                     
            let l = (rotate (-1) spins)       
            let r = (rotate 1 spins)          
            let u = (map (rotate (-1)) spins) 
            let d = (map (rotate 1) spins) 
            in map5 (map5 (\c l r u d -> 2*c*(l+r+u+d))) c l r u d
           

-- The sum of all deltas of a grid.  The result is a measure of how
-- ordered the grid is.
def delta_sum [h][w] (spins: [w][h]spin): i32 =
          let deli8 = deltas spins
          let del   = map (map i32.i8) deli8
          in reduce (+) 0 (map (reduce (+) 0) del) 


def flip (abs_temp: f32) (samplerate: f32) 
         (rng: rng_engine.rng) (deli8:i8) (c:spin) : (rng_engine.rng, spin) = 
        let (rng_tmp, a) = rand rng
        let (rng_new, b) = rand rng_tmp
        let del = f32.i8 deli8
        let b = (del < -del || b < f32.e**(-del / abs_temp))
        let c_new = if a < samplerate && b then -c else c
        in (rng_new, c_new)


-- Take one step in the Ising 2D simulation.
def step [h][w] (abs_temp: f32) (samplerate: f32)
                (rngs: [h][w]rng_engine.rng) (spins: [h][w]spin)
              : ([h][w]rng_engine.rng, [h][w]spin) =
          let dels = deltas spins
          let fliped = flip abs_temp samplerate
          let result = map3 (map3 fliped) rngs dels spins
          let (l1,l2) = unzip (flatten result)
          in (unflatten h w l1, unflatten h w l2)


-- | Just for benchmarking.
def start (abs_temp: f32) (samplerate: f32)
         (h: i64) (w: i64) (n: i32): [h][w]spin =
  (loop (rngs, spins) = random_grid 1337 h w for _i < n do
     step abs_temp samplerate rngs spins).1

-- ==
-- entry: main
-- input { 0.5f32 0.1f32 10i64 10i64 2 } auto output

-- The following definitions are for the visualisation and need not be modified.

type~ state = {cells: [][](rng_engine.rng, spin)}

entry tui_init seed h w : state =
  let (rngs, spins) = random_grid seed h w
  in {cells=map (uncurry zip) (zip rngs spins)}

entry tui_render (s: state) = map (map (.1)) s.cells

entry tui_step (abs_temp: f32) (samplerate: f32) (s: state) : state =
  let rngs = (map (map (.0)) s.cells)
  let spins = map (map (.1)) s.cells
  let (rngs', spins') = step abs_temp samplerate rngs spins
  in {cells=map (uncurry zip) (zip rngs' spins')}
