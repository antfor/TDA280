-- ==
-- input @ two_100_i32s
-- input @ two_1000_i32s
-- input @ two_10000_i32s
-- input @ two_100000_i32s
-- input @ two_1000000_i32s
-- input @ two_5000000_i32s
-- input @ two_10000000_i32s

import "lib/github.com/diku-dk/sorts/radix_sort"

def s1 : []i32 = [23,45,-23,44,23,54,23,12,34,54,7,2, 4,67]
def s2 : []i32 = [-2, 3, 4,57,34, 2, 5,56,56, 3,3,5,77,89]


entry process [n] (xs:[n]i32) (ys:[n]i32): i32 =
    reduce i32.max 0 (map2 (\x y -> i32.abs(x-y)) xs ys)

entry process_idx [n] (xs:[n]i32) (ys:[n]i32): (i32, i64) =
      let dif = map2 (\x y -> i32.abs(x-y)) xs ys
      let l = zip dif (iota n) 
      let f = (\(x,i) (y,j) -> if x<y then (y,j) else (x,i))
      in reduce f (-1,0) l        

entry e11 : i32 =
        process s1 s2

entry e13 : (i32, i64) =
        process_idx s1 s2


def segOp 't (op: t -> t -> t) (x:(t,bool)) (y:(t,bool)) : (t,bool) =
        let f1 = x.1
        let v1 = x.0
        let f2 = y.1
        let v2 = y.0
        in (if f2 then v2 else op v1 v2, f1 || f2) 


def segscan [n] 't (op: t -> t -> t) (ne: t) 
                     (arr:[n](t,bool)): *[n]t =
               map (.0) (scan (segOp op) (ne, false) arr)
                

def segreduce [n] 't (op: t -> t -> t) (ne: t)
                        (arr: [n](t, bool)): *[]t =
                let vs = segscan op ne arr
                let b = map (.1) arr 
                let b_int = map i64.bool b
                let start = rotate 1 b
                let is_unfiltered = scan (+) 0 b_int
                let is = map2 (\b i -> if b then i-1 else -1) start is_unfiltered
                let as = replicate (is_unfiltered[n-1]) ne
                in scatter as is vs
                


def main [n] (xs:[n](i32,bool)): []i32 =
    segreduce (+) 0 xs

--def main [n] (xs:[n](i32,bool)): [n]i32 =
 --   segscan (+) 0 xs


