-- ==
-- input @ one_100_two
-- input @ one_1000_two
-- input @ one_10000_two
-- input @ one_100000_two
-- input @ one_1000000_two
-- input @ one_5000000_two
-- input @ one_10000000_two


def main [n] [m] (dest :*[m]i32)  (is : [n]i64) (as : [n]i32) : *[m]i32 =
    reduce_by_index dest (+) 0  is as