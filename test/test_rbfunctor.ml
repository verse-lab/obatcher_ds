[@@@warning "-32-26"]
module IntRbtree = Obatcher_ds.Rbtree.Sequential(Int);;
module IntRbtreePrebatch = Obatcher_ds.Rbtree.Prebatch(Int);;
module IntRbtreeSplitJoin = Obatcher_ds.Splitjoin.Make(IntRbtreePrebatch);;

let num_nodes = 10000000;;
let max_key = num_nodes;;
Printf.printf "\nTesting RB tree functor with max %d nodes...\n" num_nodes;;


Printf.printf "\nStarting join test for RB tree...\n";;
let ref_array_2 = Array.make max_key @@ -1;;
let pivot = 1 + Random.full_int (max_key - 1);;
Printf.printf "Pivot: %d\n" pivot;;

let st = Sys.time();;
let rbt1 = IntRbtree.init ();;
let () = for _ = 1 to Random.full_int num_nodes do
  let k = Random.full_int pivot in
  let v = Random.full_int max_key in
  IntRbtree.insert k v rbt1;
  if ref_array_2.(k) == -1 then ref_array_2.(k) <- v
done;;

let rbt2 = IntRbtree.init ();;
let () = for _ = 1 to Random.full_int num_nodes do
  let k = pivot + 1 + Random.full_int (max_key - pivot - 1) in
  let v = Random.full_int max_key in
  IntRbtree.insert k v rbt2;
  if ref_array_2.(k) == -1 then ref_array_2.(k) <- v
done;;
Printf.printf "Insertion time for RB tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (ref_array_2.(pivot) = -1);;
assert (IntRbtree.verify_tree rbt1);;
assert (IntRbtree.verify_tree rbt2);;
let () = for i = 0 to pivot do
  if ref_array_2.(i) != -1 then
    assert (IntRbtree.search i rbt1 = Some ref_array_2.(i))
done;;
let () = for i = pivot + 1 to max_key - 1 do
  if ref_array_2.(i) != -1 then
    assert (IntRbtree.search i rbt2 = Some ref_array_2.(i))
done;;
Printf.printf "Verification time for RB trees: %fs\n" (Sys.time() -. st);;

(* IntRbtree.traverse at1;;
IntRbtree.traverse at2;; *)

let st = Sys.time();;
let mn = IntRbtree.new_node pivot 232;;
let jt = IntRbtreePrebatch.join_with_node rbt1 mn rbt2;;
Printf.printf "Join time for RB trees: %fs\n" (Sys.time() -. st);;

(* IntRbtree.traverse jt;; *)
let st = Sys.time();;
assert (IntRbtree.verify_tree jt);;
let () = for i = 0 to max_key - 1 do
  if ref_array_2.(i) != -1 then
    assert (IntRbtree.search i jt = Some ref_array_2.(i))
done;;
Printf.printf "Verification time for joined RB trees: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting split test for RB tree...\n";;
let rbt = IntRbtree.init ();;
let ref_array_1 = Array.make max_key @@ -1;;

(* Insert elements *)
let st = Sys.time();;
let () = for _ = 1 to num_nodes do
  let k = Random.full_int max_key in
  let v = Random.full_int max_key in
  IntRbtree.insert k v rbt;
  if ref_array_1.(k) == -1 then ref_array_1.(k) <- v
done;;
Printf.printf "Insertion time for RB tree: %fs\n" (Sys.time() -. st);;
(* IntRbtree.Sequential.traverse rbt;; *)

(* Verification of initial tree *)
let st = Sys.time();;
assert (let (b, _) = IntRbtree.verify_black_depth rbt.root in b);;  (* Check black depth *)
assert (IntRbtree.verify_internal_property rbt.root);;              (* Check internal property *)
let () = for i = 1 to max_key - 1 do                             (* Check whether all elements are inserted *)
  if ref_array_1.(i) != -1 then
    assert (IntRbtree.search i rbt = Some ref_array_1.(i))
done;;
Printf.printf "Verification time for RB tree: %fs\n" (Sys.time() -. st);;

(* Splitting test *)
let split_pt = Random.full_int max_key;;
let st = Sys.time();;
let (lt, mn, rt) = IntRbtreePrebatch.split_with_node rbt split_pt;;

Printf.printf "Split time for RB tree: %fs\n" (Sys.time() -. st);;
let st = Sys.time();;
assert (IntRbtree.verify_tree lt);;
assert (IntRbtree.verify_tree rt);;
let () = for i = 1 to split_pt - 1 do
  if ref_array_1.(i) != -1 then
    assert (IntRbtree.search i lt = Some ref_array_1.(i))
done;;
let () = for i = split_pt + 1 to max_key - 1 do
  if ref_array_1.(i) != -1 then
    assert (IntRbtree.search i rt = Some ref_array_1.(i))
done;;
assert (match mn with
| IntRbtree.Leaf -> ref_array_1.(split_pt) = -1
| IntRbtree.Node n' -> n'.tval = ref_array_1.(split_pt));;
Printf.printf "Verification time for split RB tree: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting deletion test for RB tree...\n";;
let at3 = IntRbtree.init ();;
let ref_array_3 = Array.make max_key @@ -1;;
let st = Sys.time();;
let num_inserted = ref 0;;
let () = for _ = 1 to max_key do
  let k = Random.full_int max_key in
  let v = Random.full_int max_key in
  IntRbtree.insert k v at3;
  if ref_array_3.(k) == -1 then (ref_array_3.(k) <- v; num_inserted := !num_inserted + 1)
done;;
Printf.printf "Inserted %d elements into RB tree\n" !num_inserted;;
Printf.printf "Insertion time for RB tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let num_removed = ref 0;;
let () = for k = 1 to max_key / 3 do
  IntRbtree.delete k at3;
  if ref_array_3.(k) != -1 then (ref_array_3.(k) <- -1; num_removed := !num_removed + 1)
done;;
Printf.printf "Removed %d elements from RB tree\n" !num_removed;;
Printf.printf "Deletion time for RB tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (IntRbtree.verify_tree at3);;
let num_found = ref 0;;
let () = for i = 0 to max_key - 1 do
  if ref_array_3.(i) != -1 then
    (assert (IntRbtree.search i at3 = Some ref_array_3.(i));
    num_found := !num_found + 1)
  else
    assert (IntRbtree.search i at3 = None)
done;;
Printf.printf "Found %d elements in RB tree after deletion\n" !num_found;;
Printf.printf "Verification time for RB tree: %fs\n" (Sys.time() -. st);;