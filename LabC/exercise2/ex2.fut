import "lib/github.com/diku-dk/sorts/radix_sort"

def segscan [n] 't (op: t -> t -> t) (ne: t) 
                   (arr: [n](t, bool)): *[n]t =
    let (res, _) = unzip (
        scan (\(v1, f1) (v2, f2) -> 
            let f = f1 || f2
            let v = if f2 then v2 else v1 `op` v2
            in (v, f)) (ne, false) arr)
    in res

def segreduce [n] 't (op: t -> t -> t) (ne: t)
                     (arr: [n](t, bool)): *[]t =
    let seg_arr   = segscan op ne arr
    let end_flags = let (_, f) = unzip arr in rotate 1 f
    let cum_sum   = (scan (+) 0 <-< map i64.bool) end_flags
    let seg_num   = if n > 0 then cum_sum[n - 1] else 0
    let index_map = 
        map2 (\v f -> if f then v - 1 else -1) cum_sum end_flags
    in
        scatter (replicate seg_num ne) index_map seg_arr

def reduce_by_index 'a [m] [n]
                    (dest: *[m]a)
                    (f: a -> a -> a) (ne: a)
                    (is: [n]i64) (as: [n]a): *[m]a =
    let tuple_arr        =
        (zip (iota m) (zip dest (replicate m true))) ++ 
        (zip is (zip as (replicate n false)))
    let sorted_tuple_arr = 
        radix_sort_by_key (\(k, _) -> k) 
            i64.num_bits i64.get_bit tuple_arr
    let (_, arr) = unzip sorted_tuple_arr
    in segreduce f ne arr :> *[m]a

def main (arr1: *[]i32) (arr2: []i64) (arr3: []i32): []i32 =
    reduce_by_index arr1 (+) 0 arr2 arr3