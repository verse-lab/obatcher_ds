[@@@warning "-32-26-35"]

module VEBTree = Obatcher_ds.Vebtree.Sequential;;

let num_nodes = 1 lsl 22;;
let max_key = num_nodes;;

(* let v = VEBTree.create max_key;;
VEBTree.insert v 668753;;
VEBTree.insert v 963397;; (* 470 * 2048 + 837*)
Printf.printf "%d\n" (Option.get @@ VEBTree.successor v 668753);;
(* VEBTree.insert v 668753;; *)
let l = VEBTree.flatten v;;
List.iter (Printf.printf "%d ") l;; *)

Printf.printf "\nStarting insertion test for vEB trees...\n";;
let arr = Array.make num_nodes false;;
let v = VEBTree.create max_key;;

let st = Sys.time();;
let () =
  for _ = 0 to max_key do
    let i = Random.int max_key in
    VEBTree.insert v i;
    arr.(i) <- true
  done;;
Printf.printf "Insertion time for vEB tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for i = 0 to num_nodes - 1 do
    if arr.(i) then
      assert (VEBTree.mem v i)
    else if VEBTree.mem v i then
      assert (not (VEBTree.mem v i))
  done;;
Printf.printf "Verification time for vEB tree: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting deletion test for vEB trees...\n";;

let arr = Array.make num_nodes false;;
let v = VEBTree.create max_key;;

let st = Sys.time();;
let num_inserted = ref 0;;
let () =
  for _ = 0 to max_key do
    let i = Random.int max_key in
    VEBTree.insert v i;
    if not arr.(i) then
      (arr.(i) <- true;
      num_inserted := !num_inserted + 1)
  done;;
Printf.printf "Insertion time for vEB tree: %fs\n" (Sys.time() -. st);;
Printf.printf "Elements inserted: %ds\n" !num_inserted;;

let st = Sys.time();;
let num_del = ref 0;;
let () =
  for _ = 0 to max_key / 2 do
    let i = Random.int max_key in
    VEBTree.delete v i;
    if arr.(i) then
      (arr.(i) <- false;
      num_del := !num_del + 1)
  done;;
Printf.printf "Deletion time for vEB tree: %fs\n" (Sys.time() -. st);;
Printf.printf "Elements deleted: %ds\n" !num_del;;

let st = Sys.time();;
let num_found = ref 0;;
let () =
  for i = 0 to num_nodes - 1 do
    if arr.(i) then
      (assert (VEBTree.mem v i); num_found := !num_found + 1)
    else
      assert (not (VEBTree.mem v i))
  done;;
Printf.printf "Verification time for vEB tree: %fs\n" (Sys.time() -. st);;
Printf.printf "Elements found: %ds\n" !num_found;;


Printf.printf "\nStarting successor test for vEB trees...\n";;
let arr = Array.make num_nodes false;;
let v = VEBTree.create max_key;;

let st = Sys.time();;
let () =
  for _ = 0 to max_key do
    let i = Random.int max_key in
    if not arr.(i) then
      (VEBTree.insert v i;
      arr.(i) <- true)
  done;;
Printf.printf "Insertion time for vEB tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for _ = 0 to max_key / 2 do
    let i = Random.int max_key in
    VEBTree.delete v i;
    if arr.(i) then arr.(i) <- false
  done;;
Printf.printf "Deletion time for vEB tree: %fs\n" (Sys.time() -. st);;

let succ_arr = Array.make num_nodes None;;
let cur_succ = ref None;;
let st = Sys.time();;
let () =
  for i = max_key - 1 downto 0 do
    succ_arr.(i) <- !cur_succ;
    if arr.(i) then cur_succ := Some i
  done;;
Printf.printf "Initialization time for successor array: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for i = 0 to num_nodes - 1 do
    assert (VEBTree.successor v i = succ_arr.(i))
  done;;
Printf.printf "Verification time for successor test: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting predecessor test for vEB trees...\n";;
let arr = Array.make num_nodes false;;
let v = VEBTree.create max_key;;

let st = Sys.time();;
let () =
  for _ = 0 to max_key do
    let i = Random.int max_key in
    if not arr.(i) then
      (VEBTree.insert v i;
      arr.(i) <- true)
  done;;
Printf.printf "Insertion time for vEB tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for _ = 0 to max_key / 2 do
    let i = Random.int max_key in
    VEBTree.delete v i;
    if arr.(i) then arr.(i) <- false
  done;;
Printf.printf "Deletion time for vEB tree: %fs\n" (Sys.time() -. st);;

let pred_arr = Array.make num_nodes None;;
let cur_pred = ref None;;
let st = Sys.time();;
let () =
  for i = 0 to max_key - 1 do
    pred_arr.(i) <- !cur_pred;
    if arr.(i) then cur_pred := Some i
  done;;
Printf.printf "Initialization time for predecessor array: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for i = 0 to num_nodes - 1 do
    assert (VEBTree.predecessor v i = pred_arr.(i))
  done;;
Printf.printf "Verification time for predecessor test: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting flatten test for vEB trees...\n";;
let arr = Array.make num_nodes false;;
let v = VEBTree.create max_key;;

let st = Sys.time();;
let () =
  for _ = 0 to max_key do
    let i = Random.int max_key in
    if not arr.(i) then
      (VEBTree.insert v i;
      arr.(i) <- true)
  done;;
Printf.printf "Insertion time for vEB tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for _ = 0 to max_key / 2 do
    let i = Random.int max_key in
    VEBTree.delete v i;
    if arr.(i) then arr.(i) <- false
  done;;
Printf.printf "Deletion time for vEB tree: %fs\n" (Sys.time() -. st);;

let new_list = ref [];;
let () =
  for i = num_nodes - 1 downto 0 do
    if arr.(i) then new_list := i :: !new_list
  done;;

let st = Sys.time();;
let l = VEBTree.flatten v;;
(* let larr = Array.of_list l;;
for i = 0 to Array.length larr - 1 do
  Printf.printf "%d " larr.(i)
done;;
Printf.printf "\n";;
let new_arr = Array.of_list !new_list;;
for i = 0 to Array.length new_arr - 1 do
  Printf.printf "%d " new_arr.(i)
done;;
Printf.printf "\n";; *)
Printf.printf "Flatten time for vEB tree: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
assert (l = !new_list);;
Printf.printf "Verification time for vEB tree: %fs\n" (Sys.time() -. st);;