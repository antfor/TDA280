-- ==
-- input @ two_100_i32s
-- input @ two_1000_i32s
-- input @ two_10000_i32s
-- input @ two_100000_i32s
-- input @ two_1000000_i32s
-- input @ two_5000000_i32s
-- input @ two_10000000_i32s

def process_idx [n] (xs: [n]i32) (ys: [n]i32): (i32, i64) =
    let max (n1, i1) (n2, i2) =
        if      n1 > n2 then (n1, i1)
        else if n1 < n2 then (n2, i2)
        else if i1 > i2 then (n1, i1)
        else                 (n2, i2)
    in
        reduce_comm max (0, -1)
            (zip (map2 (\x y -> i32.abs (x - y)) xs ys) (iota n))

def main (xs: []i32) (ys: []i32): (i32, i64) =
    process_idx xs ys