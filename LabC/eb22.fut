-- ==
-- input @ one_100_i32s
-- input @ one_1000_i32s
-- input @ one_10000_i32s
-- input @ one_100000_i32s
-- input @ one_1000000_i32s
-- input @ one_5000000_i32s
-- input @ one_10000000_i32s

def main [n] (arr:[n]i32) =
    reduce (+) 0 arr