

def main [n] [m] (dest :*[m]i32)  (is : [n]i64) (as : [n]i32) : *[m]i32 =
    reduce_by_index dest (+) 0  is as