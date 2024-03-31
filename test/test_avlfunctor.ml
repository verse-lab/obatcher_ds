module IntAvltree = Obatcher_ds.Avltree.Sequential(Int);;
module IntAvltreePrebatch = Obatcher_ds.Avltree.Prebatch(Int);;
module IntAvltreeSplitJoin = Obatcher_ds.Splitjoin.Make(IntAvltreePrebatch);;

let num_nodes = 11000000;;
let max_key = num_nodes;;
let num_pivots = 10000;;

Printf.printf "\nStarting split test for AVL trees...\n";;

let at = IntAvltree.init ();;
let ref_array_2 = Array.make max_key @@ -1;;

let st = Sys.time();;
let () = for _ = 1 to num_nodes do
  let k = Random.full_int max_key in
  let v = Random.full_int max_key in
  IntAvltree.insert k v at;
  if ref_array_2.(k) == -1 then ref_array_2.(k) <- v
done;;

Printf.printf "Insertion time for AVL tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (IntAvltree.verify_height_invariant at.root);;
let () = for i = 0 to max_key - 1 do
  if ref_array_2.(i) != -1 then
    assert (IntAvltree.search i at = Some ref_array_2.(i))
  else assert (IntAvltree.search i at = None)
done;;
Printf.printf "Verification time for AVL tree: %fs\n" (Sys.time() -. st);;

let pivot_arr = Array.init num_pivots (fun _ -> Random.full_int max_key);;
Array.sort Int.compare pivot_arr;;

let st = Sys.time();;
let split_arr = IntAvltreeSplitJoin.split pivot_arr at;;
Printf.printf "Split time for AVL tree: %fs\n" (Sys.time() -. st);;

Printf.printf "%d\n" (Array.length pivot_arr);;
Printf.printf "%d\n" (Array.length split_arr);;
assert(Array.length split_arr = num_pivots + 1);;
let cur_pivot_idx = ref 0;;
let st = Sys.time();;
let () = for i = 0 to max_key - 1 do
  while !cur_pivot_idx < Array.length pivot_arr && pivot_arr.(!cur_pivot_idx) = i do
    cur_pivot_idx := !cur_pivot_idx + 1;
  done;
  if ref_array_2.(i) != -1 then
    assert (IntAvltree.search i split_arr.(!cur_pivot_idx) = Some ref_array_2.(i))
  else assert (IntAvltree.search i split_arr.(!cur_pivot_idx) = None)
done;;
Printf.printf "Verification time for split AVL tree: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting deletion test for AVL...\n";;

let at3 = IntAvltree.init ();;
let ref_array_3 = Array.make max_key @@ -1;;
let st = Sys.time();;
let num_inserted = ref 0;;
let () = for _ = 1 to max_key do
  let k = Random.full_int max_key in
  let v = Random.full_int max_key in
  IntAvltree.insert k v at3;
  if ref_array_3.(k) == -1 then (ref_array_3.(k) <- v; num_inserted := !num_inserted + 1)
done;;
Printf.printf "Inserted %d elements into AVL tree\n" !num_inserted;;
Printf.printf "Insertion time for AVL tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let num_removed = ref 0;;
let () = for k = 1 to max_key / 3 do
  IntAvltree.delete k at3;
  if ref_array_3.(k) != -1 then (ref_array_3.(k) <- -1; num_removed := !num_removed + 1)
done;;
Printf.printf "Removed %d elements from AVL tree\n" !num_removed;;
Printf.printf "Deletion time for AVL tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (IntAvltree.verify_height_invariant at3.root);;
let num_found = ref 0;;
let () = for i = 0 to max_key - 1 do
  if ref_array_3.(i) != -1 then
    (assert (IntAvltree.search i at3 = Some ref_array_3.(i));
    num_found := !num_found + 1)
  else
    assert (IntAvltree.search i at3 = None)
done;;
Printf.printf "Found %d elements in AVL tree after deletion\n" !num_found;;
Printf.printf "Verification time for AVL tree: %fs\n" (Sys.time() -. st);;