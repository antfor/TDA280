-- ==
-- input @ i32_100_bools
-- input @ i32_1000_bools
-- input @ i32_10000_bools
-- input @ i32_100000_bools
-- input @ i32_1000000_bools
-- input @ i32_5000000_bools
-- input @ i32_10000000_bools
import "labc"


def main [n] (vs:[n]i32) (bs:[n]bool) =
    let arr = zip vs bs 
    in segscan (+) 0 arr