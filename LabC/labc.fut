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
                let len = if n==0 then 0 else is_unfiltered[n-1]
                let as = replicate len ne
                in scatter as is vs

def segreduce2 [n] 't (op: t -> t -> t) (ne: t)
                        (arr: [n](t, bool)): *[]t =
                let vs = segscan op ne arr
                let b = map (.1) arr 
                let b_int = map i64.bool b
                let start = rotate 1 b
                let is_unfiltered = scan (+) 0 b_int
                let is = map2 (\b i -> if b then i-1 else -1) start is_unfiltered
                let len = is_unfiltered[n-1]
                let as = replicate len ne :> *[len]t
                in scatter as is vs :> *[len]t         

-- works if radix_sort_by_key is stable
def reduce_by_index1 'a [m] [n]
                        (dest : *[m]a)
                        (f : a -> a -> a) (ne : a)
                        (is : [n]i64) (as : [n]a) : *[m]a = 
                let in_vs = zip3 (iota m) dest (replicate m true)
                let is_as = zip3 is as (replicate n false)
                let npm = n + m
                let all_unsorted = concat_to npm in_vs is_as
                let all = radix_sort_by_key (.0) i64.num_bits i64.get_bit all_unsorted 
                let flags = map (.2) all
                let values = map (.1) all
                in  segreduce f ne (zip values flags) :> *[m]a

def main : [](i64,i32) =
    radix_sort_by_key (.0) i64.num_bits i64.get_bit [(3,1),(2,2),(1,3)]


--def main [n] (xs:[n](i32,bool)): []i32 =
--    segreduce (+) 0 xs

--def main [n] (xs:[n](i32,bool)): [n]i32 =
 --   segscan (+) 0 xs


