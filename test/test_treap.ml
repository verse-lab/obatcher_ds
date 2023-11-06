[@@@warning "-32-26"]
module IntTreap = Obatcher_ds.Treap.Make(Int);;
let num_nodes = 10000000;;
let max_key = num_nodes;;
Printf.printf "\nTesting Treap with max %d nodes...\n" num_nodes;;


Printf.printf "\nStarting join test for Treap...\n";;
let ref_array_2 = Array.make max_key @@ -1;;
let pivot = 1 + Random.full_int (max_key - 1);;
Printf.printf "Pivot: %d\n" pivot;;

let st = Sys.time();;
let at1 = IntTreap.Sequential.new_tree ();;
let () = for _ = 1 to Random.full_int num_nodes do
  let k = Random.full_int pivot in
  let v = Random.full_int max_key in
  IntTreap.Sequential.insert k v at1;
  if ref_array_2.(k) == -1 then ref_array_2.(k) <- v
done;;

let at2 = IntTreap.Sequential.new_tree ();;
let () = for _ = 1 to Random.full_int num_nodes do
  let k = pivot + 1 + Random.full_int (max_key - pivot - 1) in
  let v = Random.full_int max_key in
  IntTreap.Sequential.insert k v at2;
  if ref_array_2.(k) == -1 then ref_array_2.(k) <- v
done;;
Printf.printf "Insertion time for Treap: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (ref_array_2.(pivot) = -1);;
assert (IntTreap.Sequential.verify_heap_property_aux at1.root);;
assert (IntTreap.Sequential.verify_heap_property_aux at2.root);;
let () = for i = 0 to pivot do
  if ref_array_2.(i) != -1 then
    assert (IntTreap.Sequential.search i at1 = Some ref_array_2.(i))
done;;
let () = for i = pivot + 1 to max_key - 1 do
  if ref_array_2.(i) != -1 then
    assert (IntTreap.Sequential.search i at2 = Some ref_array_2.(i))
done;;
Printf.printf "Verification time for Treaps: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let mn = IntTreap.Sequential.new_node pivot 232;;
let jt = IntTreap.Sequential.join at1 mn at2;;
Printf.printf "Join time for Treaps: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (IntTreap.Sequential.verify_heap_property_aux jt.root);;
let () = for i = 0 to max_key - 1 do
  if ref_array_2.(i) != -1 then
    assert (IntTreap.Sequential.search i jt = Some ref_array_2.(i))
done;;
Printf.printf "Verification time for joined Treaps: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting split test for Treaps...\n";;

let at = IntTreap.Sequential.new_tree ();;
let ref_array_2 = Array.make max_key @@ -1;;

let st = Sys.time();;
let () = for _ = 1 to num_nodes / 2 do
  let k = Random.full_int (max_key / 2) in
  let v = Random.full_int (max_key / 2) in
  IntTreap.Sequential.insert k v at;
  if ref_array_2.(k) == -1 then ref_array_2.(k) <- v
done;;

let () = for _ = 1 to num_nodes / 2 do
  let k = Random.full_int (max_key / 2) + (max_key / 2)in
  let v = Random.full_int (max_key / 2) + (max_key / 2)in
  IntTreap.Sequential.insert k v at;
  if ref_array_2.(k) == -1 then ref_array_2.(k) <- v
done;;
Printf.printf "Insertion time for Treap: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (IntTreap.Sequential.verify_heap_property_aux at.root);;
let () = for i = 1 to max_key - 1 do
  if ref_array_2.(i) != -1 then
    assert (IntTreap.Sequential.search i at = Some ref_array_2.(i))
done;;
Printf.printf "Verification time for Treap: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let split_pt = Random.full_int max_key;;
Printf.printf "Split at %d\n" split_pt;;
let (lt, mn, rt) = IntTreap.Sequential.split at (split_pt);;
Printf.printf "Split time for Treap: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (IntTreap.Sequential.verify_heap_property_aux lt.root);;
assert (IntTreap.Sequential.verify_heap_property_aux rt.root);;
let () = for i = 1 to split_pt - 1 do
  if ref_array_2.(i) != -1 then
    assert (IntTreap.Sequential.search i lt = Some ref_array_2.(i))
done;;
let () = for i = split_pt + 1 to max_key - 1 do
  if ref_array_2.(i) != -1 then
    assert (IntTreap.Sequential.search i rt = Some ref_array_2.(i))
done;;
assert (match mn with
| IntTreap.Sequential.Leaf -> ref_array_2.(split_pt) = -1
| IntTreap.Sequential.Node n' -> n'.nval = ref_array_2.(split_pt));;
Printf.printf "Verification time for split Treap: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting deletion test for Treap...\n";;
let ref_array_3 = Array.make max_key @@ -1;;
let st = Sys.time();;
let at3 = IntTreap.Sequential.new_tree ();;
let num_inserted = ref 0;;
let () = for _ = 1 to max_key do
  let k = Random.full_int max_key in
  let v = Random.full_int max_key in
  IntTreap.Sequential.insert k v at3;
  if ref_array_3.(k) == -1 then (ref_array_3.(k) <- v; num_inserted := !num_inserted + 1)
done;;
Printf.printf "Inserted %d elements into Treap\n" !num_inserted;;
Printf.printf "Insertion time for Treap: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let num_removed = ref 0;;
let () = for k = 1 to max_key / 3 do
  IntTreap.Sequential.delete k at3;
  if ref_array_3.(k) != -1 then (ref_array_3.(k) <- -1; num_removed := !num_removed + 1)
done;;
Printf.printf "Removed %d elements from Treap\n" !num_removed;;
Printf.printf "Deletion time for Treap: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (IntTreap.Sequential.verify_heap_property_aux at3.root);;
let num_found = ref 0;;
let () = for i = 0 to max_key - 1 do
  if ref_array_3.(i) != -1 then
    (assert (IntTreap.Sequential.search i at3 = Some ref_array_3.(i));
    num_found := !num_found + 1)
  else
    assert (IntTreap.Sequential.search i at3 = None)
done;;
Printf.printf "Found %d elements in Treap after deletion\n" !num_found;;
Printf.printf "Verification time for Treap: %fs\n" (Sys.time() -. st);;