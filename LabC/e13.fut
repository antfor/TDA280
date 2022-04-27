-- ==
-- input @ two_100_i32s
-- input @ two_1000_i32s
-- input @ two_10000_i32s
-- input @ two_100000_i32s
-- input @ two_1000000_i32s
-- input @ two_5000000_i32s
-- input @ two_10000000_i32s


def process_idx [n] (xs:[n]i32) (ys:[n]i32): (i32, i64) =
      let dif = map2 (\x y -> i32.abs(x-y)) xs ys
      let l = zip dif (iota n) 
      let f = (\(x,i) (y,j) -> if x<y then (y,j) else (x,i))
      in reduce f (-1,0) l   
      

def main [n] (xs:[n]i32) (ys:[n]i32): (i32, i64) =
    process_idx xs ys