[@@@warning "-32-26-35"]

(* module Xfast = Obatcher_ds.Xfast.Sequential(Obatcher_ds.Yfast.HashXfastTabl);; *)
module Xfast = Obatcher_ds.Xfast.Sequential(Obatcher_ds.Xfast.ArrXfastTabl);;

let int_size = 22;;
let max_int = 4000000;;

(* let t = Xfast.init ~int_size ();; *)

Printf.printf "\nStarting insertion test for Xfast trie...\n";;
let arr = Array.make max_int false;;
let t = Xfast.init int_size;;

let st = Sys.time();;
let () =
  for _ = 0 to max_int do
    let i = Random.int max_int in
    Xfast.insert t i;
    arr.(i) <- true
  done;;
Printf.printf "Insertion time for Xfast trie: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for _ = 0 to max_int / 2 do
    let i = Random.int max_int in
    Xfast.delete t i;
    if arr.(i) then arr.(i) <- false
  done;;
Printf.printf "Deletion time for Xfast trie: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for i = 0 to max_int - 1 do
    if arr.(i) then
      assert (Xfast.mem t i)
    else if Xfast.mem t i then
      assert (not (Xfast.mem t i))
  done;;
Printf.printf "Verification time for Xfast trie: %fs\n" (Sys.time() -. st);;



Printf.printf "\nStarting successor test for Xfast tries...\n";;
let arr = Array.make max_int false;;
let t = Xfast.init int_size;;

let st = Sys.time();;
let () =
  for _ = 0 to max_int do
    let i = Random.int max_int in
    if not arr.(i) then
      (Xfast.insert t i;
      arr.(i) <- true)
  done;;
Printf.printf "Insertion time for Xfast trie: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for _ = 0 to max_int / 2 do
    let i = Random.int max_int in
    Xfast.delete t i;
    if arr.(i) then arr.(i) <- false
  done;;
Printf.printf "Deletion time for Xfast trie: %fs\n" (Sys.time() -. st);;

let succ_arr = Array.make max_int None;;
let cur_succ = ref None;;
let st = Sys.time();;
let () =
  for i = max_int - 1 downto 0 do
    succ_arr.(i) <- !cur_succ;
    if arr.(i) then cur_succ := Some i
  done;;
Printf.printf "Initialization time for successor array: %fs\n" (Sys.time() -. st);;

(* Printf.printf "[";;
let () = for i = 0 to max_int - 1 do
  (* if arr.(i) then *)
    Printf.printf "(%d, %d); " i (if succ_arr.(i) != None then Option.get succ_arr.(i) else -1)
  (* else Printf.printf "()" *)
done;;
Printf.printf "]\n";; *)

let st = Sys.time();;
(* Printf.printf "[";; *)
let () =
  for i = 0 to max_int - 1 do
    (* Printf.printf "(%d, %d); " i (let s = Xfast.successor t i in if s != None then Option.get s else -1) *)
    assert (Xfast.successor t i = succ_arr.(i))
  done;;
(* Printf.printf "]\n";; *)
Printf.printf "Verification time for successor test: %fs\n" (Sys.time() -. st);;


Printf.printf "\nStarting predecessor test for Xfast tries...\n";;
let arr = Array.make max_int false;;
let t = Xfast.init int_size;;

let st = Sys.time();;
let () =
  for _ = 0 to max_int do
    let i = Random.int max_int in
    if not arr.(i) then
      (Xfast.insert t i;
      arr.(i) <- true)
  done;;
Printf.printf "Insertion time for Xfast trie: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for _ = 0 to max_int / 2 do
    let i = Random.int max_int in
    Xfast.delete t i;
    if arr.(i) then arr.(i) <- false
  done;;
Printf.printf "Deletion time for Xfast trie: %fs\n" (Sys.time() -. st);;

let pred_arr = Array.make max_int None;;
let cur_pred = ref None;;
let st = Sys.time();;
let () =
  for i = 0 to max_int - 1 do
    pred_arr.(i) <- !cur_pred;
    if arr.(i) then cur_pred := Some i
  done;;
Printf.printf "Initialization time for predecessor array: %fs\n" (Sys.time() -. st);;

let st = Sys.time();;
let () =
  for i = 0 to max_int - 1 do
    assert (Xfast.predecessor t i = pred_arr.(i))
  done;;
Printf.printf "Verification time for predecessor test: %fs\n" (Sys.time() -. st);;


(* Xfast.insert t 100;; *)
(* let prefix = Xfast.longest_prefix_node t 50;;
Printf.printf "%d\n" (Option.get prefix.value);;
Printf.printf "%d\n" (prefix.lvl);;
assert (prefix.has_right);; *)
(* Xfast.insert t 150; *)
(* Xfast.insert t 100;
Xfast.insert t 150; *)
(* assert (Xfast.mem t 100);;
assert (Xfast.successor t 50 = Some 100);;
assert (Xfast.predecessor t 150 = Some 100);;
Printf.printf "%d\n" (Option.get @@ Xfast.predecessor t 150);;
Printf.printf "%d\n" (Option.get @@ Xfast.predecessor t 151);;
assert (Xfast.predecessor t 151 = Some 150);;

Xfast.insert t 50;;
assert (Xfast.mem t 100);;
assert (Xfast.mem t 50);;
assert (Xfast.mem t 150);;
assert (Xfast.predecessor t 50 = None);;
assert (Xfast.successor t 50 = Some 100);; *)
(* assert (Xfast.successor t 100 = Some 150);; *)
(* let predtest = Xfast.successor t 100;;
Printf.printf "%d\n" (Option.get predtest);; *)
(* assert (Xfast.predecessor t 100 = Some 150);; *)
(* assert (Xfast.predecessor t 150 = Some 100);; *)
(* Printf.printf "Yay!\n";; *)