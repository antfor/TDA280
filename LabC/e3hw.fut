-- ==
-- input { 0.5f32 0.1f32 100i64 100i64 20 } auto output
-- input { 0.5f32 0.1f32 200i64 200i64 20 } auto output
-- input { 0.5f32 0.1f32 400i64 400i64 20 } auto output
-- input { 0.5f32 0.1f32 800i64 800i64 20 } auto output
-- input { 0.5f32 0.1f32 1600i64 1600i64 20 } auto output


import "3/ising"



def main  (abs_temp: f32) (samplerate: f32)
         (h: i64) (w: i64) (n: i32): [h][w]spin =
         start abs_temp samplerate h w n