

import "labc"

def main [n] [m] (dest :*[m]i32)  (is : [n]i64) (as : [n]i32) : *[m]i32 =
    reduce_by_index1 dest (+) 0  is as