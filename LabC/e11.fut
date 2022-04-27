-- ==
-- input @ two_100_i32s
-- input @ two_1000_i32s
-- input @ two_10000_i32s
-- input @ two_100000_i32s
-- input @ two_1000000_i32s
-- input @ two_5000000_i32s
-- input @ two_10000000_i32s

def process [n] (xs:[n]i32) (ys:[n]i32): i32 =
    reduce i32.max 0 (map2 (\x y -> i32.abs(x-y)) xs ys)
    
    
def main [n] (xs:[n]i32) (ys:[n]i32): i32 =
    process xs ys