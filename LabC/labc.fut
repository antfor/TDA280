

def s1 : []i32 = [23,45,-23,44,23,54,23,12,34,54,7,2, 4,67]
def s2 : []i32 = [-2, 3, 4,57,34, 2, 5,56,56, 3,3,5,77,89]


def process [n] (xs:[n]i32) (ys:[n]i32): i32 =
    reduce i32.max 0 (map2 (\x y -> i32.abs(x-y)) xs ys)

def process_idx [n] (xs:[n]i32) (ys:[n]i32): (i32, i64) =
      let dif = map2 (\x y -> i32.abs(x-y)) xs ys
      let l = zip dif (iota n) 
      let f = (\(x,i) (y,j) -> if x<y then (y,j) else (x,i))
      in reduce f (-1,-1) l


entry e11 : i32 =
        process s1 s2

entry e13 : (i32, i64) =
        process_idx s1 s2

        
def main : i32 =
    process s1 s2


