[@@@warning "-32-26-39-27"]

(* Use resizeable hash table for the x-fast trie portion of the y-fast trie *)
module HashXfastTabl = struct

  type 'a t = {
    max_lvl : int;
    arr : (int, 'a) Hashtbl.t array
  }
  let create n_levels = {
    max_lvl = n_levels - 1;
    arr = Array.init n_levels (fun _ -> Hashtbl.create 1)
  }

  let find_in_level t lvl x = Hashtbl.find_opt t.arr.(lvl) x

  let set_at_level t lvl x n =
    Hashtbl.remove t.arr.(lvl) x;
    Hashtbl.add t.arr.(lvl) x n

  let remove_from_level t lvl x = Hashtbl.remove t.arr.(lvl) x

  let previous_at_level t lvl x =
    if lvl < 0 || lvl > t.max_lvl then failwith "Invalid level for trie";
    let idx = Int.shift_right_logical x (t.max_lvl - lvl) in
    Int.shift_left (idx - 1) (t.max_lvl - lvl)

  let flatten t =
    List.sort Int.compare @@
      Hashtbl.fold (fun i _ l -> i :: l) t.arr.(t.max_lvl) []
end

module Sequential = struct

  type kt = Int.t

  module Xfast = Xfast.Sequential(HashXfastTabl)
  module Rbtree = Rbgeneral.Prebatch(Int)

  type t = {
    int_size : int;
    xfast_layer : Xfast.t;
    rb_layer : (int, unit Rbtree.S.t) Hashtbl.t
  }

  let init int_size =
    (* let int_size = 
      match int_size with
      | None -> Sys.int_size - 1 (* Note that Sys.int_size = 63, minus one (62) to account for sign bit. *)
      | Some i -> if i > 0 && i <= Sys.int_size - 1 then i else failwith "Invalid int size" in *)
    let t = {
      int_size;
      xfast_layer = Xfast.init int_size;
      rb_layer = Hashtbl.create 1
    } in
    Xfast.insert t.xfast_layer 0;
    Hashtbl.add t.rb_layer 0 @@ Rbtree.S.init ();
    t

  let mem t x =
    let rb_idx =
      if Xfast.mem t.xfast_layer x then x
      else Option.get @@ Xfast.predecessor t.xfast_layer x in
    let rb = Hashtbl.find t.rb_layer rb_idx in
    match Rbtree.S.search x rb with
    | None -> false
    | Some _ -> true

  (* Handle case where rb_idx is 0 *)
  let insert_rb_tree t rb_idx rb =
    (* let min_key = Option.get @@ Rbtree.S.find_min_key rb in *)
    Xfast.insert t.xfast_layer rb_idx;
    Hashtbl.add t.rb_layer rb_idx rb

  let remove_rb_tree t rb_idx =
    Xfast.delete t.xfast_layer rb_idx;
    Hashtbl.remove t.rb_layer rb_idx

  let split_rb_tree rb rb_idx =
    (* Printf.printf "Splitting...\n"; *)
    let rb_min = Option.get @@ Rbtree.S.find_min_key rb in
    let rb_max = Option.get @@ Rbtree.S.find_max_key rb in
    (* let rb_flat = Rbtree.S.flatten rb in
    Printf.printf "[";
    List.iter (fun (i, _) -> Printf.printf "%d, " i) rb_flat;
    Printf.printf "]\n";
    Printf.printf "Woohoo\n"; *)
    let pivot = rb_min + (rb_max - rb_min) / 2 in
    (* Printf.printf "Pivot is %d\n" pivot; *)
    let left_tree, right_tree = Rbtree.split_two rb pivot in
    (* let left_flat = Rbtree.S.flatten left_tree in
    let right_flat = Rbtree.S.flatten right_tree in
    Printf.printf "Left: [";
    List.iter (fun (i, _) -> Printf.printf "%d, " i) left_flat;
    Printf.printf "]\n";
    Printf.printf "Right: [";
    List.iter (fun (i, _) -> Printf.printf "%d, " i) right_flat;
    Printf.printf "]\n"; *)
    let left_min =
      if rb_idx = 0 then 0
      else Option.get @@ Rbtree.S.find_min_key left_tree in
    (* Printf.printf "Done splitting...\n"; *)
    ((left_min, left_tree), (Option.get @@ Rbtree.S.find_min_key right_tree, right_tree))

  let insert_aux split t x =
    let rb_idx =
      if Xfast.mem t.xfast_layer x then x
      else Option.get @@ Xfast.predecessor t.xfast_layer x in
    let rb = Hashtbl.find t.rb_layer rb_idx in
    Rbtree.S.insert x () rb;

    (* Split the RB tree when needed *)
    if split && Rbtree.S.size rb > 2 * t.int_size then begin
      let (left_idx, left_rb), (right_idx, right_rb) =
        split_rb_tree rb rb_idx in
      remove_rb_tree t rb_idx;
      insert_rb_tree t left_idx left_rb;
      insert_rb_tree t right_idx right_rb
    end

  let insert = insert_aux true

  let predecessor t x =
    let rb_tree_idx =
      if Xfast.mem t.xfast_layer x then x
      else Option.get @@ Xfast.predecessor t.xfast_layer x in
    let rb_tree = Hashtbl.find t.rb_layer rb_tree_idx in
    let cur_pred = Rbtree.S.predecessor x rb_tree in
    if cur_pred = None then
      Option.bind
        (Xfast.predecessor t.xfast_layer rb_tree_idx)
        (fun idx -> Rbtree.S.find_max_key @@ Hashtbl.find t.rb_layer idx)
    else cur_pred

  let successor t x =
    let rb_tree_idx =
      if Xfast.mem t.xfast_layer x then x
      else Option.get @@ Xfast.predecessor t.xfast_layer x in
    let rb_tree = Hashtbl.find t.rb_layer rb_tree_idx in
    let cur_succ = Rbtree.S.successor x rb_tree in
    if cur_succ = None then
      Option.bind
        (Xfast.successor t.xfast_layer rb_tree_idx)
        (fun idx -> Rbtree.S.find_min_key @@ Hashtbl.find t.rb_layer idx)
    else cur_succ

  let delete t x =
    let rb_idx =
      if Xfast.mem t.xfast_layer x then x
      else Option.get @@ Xfast.predecessor t.xfast_layer x in
    let rb_tree = Hashtbl.find t.rb_layer rb_idx in
    Rbtree.S.delete x rb_tree;

    (* Merge RB trees if needed *)
    if Rbtree.S.size rb_tree < t.int_size / 2 then begin
      let rb_idx_pred = Xfast.predecessor t.xfast_layer rb_idx in
      let rb_idx_succ = Xfast.successor t.xfast_layer rb_idx in

      if rb_idx_pred != None then begin
        let rb_pred = Hashtbl.find t.rb_layer @@ Option.get rb_idx_pred in
        let new_rb = Rbtree.join_two rb_pred rb_tree in
        remove_rb_tree t @@ Option.get rb_idx_pred;
        remove_rb_tree t rb_idx;
        let new_rb_min =
          if Option.get rb_idx_pred = 0 then 0
          else Option.get @@ Rbtree.S.find_min_key new_rb in
        if Rbtree.S.size new_rb > 2 * t.int_size then begin
          let (left_idx, left_rb), (right_idx, right_rb) =
            split_rb_tree new_rb new_rb_min in
          insert_rb_tree t left_idx left_rb;
          insert_rb_tree t right_idx right_rb
        end else insert_rb_tree t new_rb_min new_rb;

      end else if Xfast.successor t.xfast_layer rb_idx != None then begin
        let rb_succ = Hashtbl.find t.rb_layer @@ Option.get rb_idx_succ in
        let new_rb = Rbtree.join_two rb_tree rb_succ in
        remove_rb_tree t @@ Option.get rb_idx_succ;
        remove_rb_tree t rb_idx;
        let new_rb_min =
          if rb_idx = 0 then 0
          else Option.get @@ Rbtree.S.find_min_key new_rb in
        if Rbtree.S.size new_rb > 2 * t.int_size then begin
          let (left_idx, left_rb), (right_idx, right_rb) =
            split_rb_tree new_rb new_rb_min in
          insert_rb_tree t left_idx left_rb;
          insert_rb_tree t right_idx right_rb
        end else insert_rb_tree t new_rb_min new_rb;
      end
    end

  let flatten t =
    let idx_arr = List.rev @@ HashXfastTabl.flatten t.xfast_layer.tables in
    let res = ref [] in
    List.iter
      (fun idx ->
        let l = Rbtree.S.flatten @@ Hashtbl.find t.rb_layer idx in
        res := List.map fst l @ !res)
      idx_arr;
    !res

end

module Prebatch = struct

  module S = Sequential

  (* module Ints = Set.Make(Int) *)
  type dt = unit
  (* type dt = {
    mutable rem : Ints.t;
    mutable add : (int * unit S.Rbtree.S.t) list
  } *)

  let compare = Int.compare

  let deduplicate ops: S.kt array =
    let new_ops_list = ref [] in
    for i = Array.length ops - 1 downto 0 do
      if i = 0 || ops.(i) <> ops.(i - 1)
      then new_ops_list := ops.(i) :: !new_ops_list
    done; Array.of_list !new_ops_list

  (* let make_pivots (t: S.t) (arr: S.kt array) =
    Array.init
      (Array.length arr)
      (fun i ->
        if S.Xfast.mem t.xfast_layer arr.(i) then arr.(i)
        else Option.get @@ S.Xfast.predecessor t.xfast_layer arr.(i)) *)

  let expose_t (t: S.t) (arr: S.kt array) =
    deduplicate @@ Array.init
      (Array.length arr)
      (fun i ->
        if S.Xfast.mem t.xfast_layer arr.(i) then arr.(i)
        else Option.get @@ S.Xfast.predecessor t.xfast_layer arr.(i)), ()
    (* Array.init (Array.length arr + 1) (fun _ -> {rem = Ints.empty; add = []}) *)

  (* let insert_dt_aux (t: S.t) dt x =
    let rb_idx =
      if S.Xfast.mem t.xfast_layer x then x
      else Option.get @@ S.Xfast.predecessor t.xfast_layer x in
    let rb = Hashtbl.find t.rb_layer rb_idx in
    S.Rbtree.S.insert x () rb *)
    (* if S.Rbtree.S.size rb > 2 * t.int_size then
      dt.rem <- Ints.add rb_idx dt.rem *)

  let insert_t (t: S.t) arr dt range =
    for i = fst range to snd range - 1 do
      let x = arr.(i) in
      let rb_idx =
        if S.Xfast.mem t.xfast_layer x then x
        else Option.get @@ S.Xfast.predecessor t.xfast_layer x in
      let rb = Hashtbl.find t.rb_layer rb_idx in
      S.Rbtree.S.insert x () rb
      (* insert_dt_aux t dt arr.(i) *)
    done

  let rec split_rb_tree_and_insert (t: S.t) rb rb_idx =
    let (left_idx, left_rb), (right_idx, right_rb) =
      S.split_rb_tree rb rb_idx in
    if S.Rbtree.S.size left_rb > 2 * t.int_size then
      split_rb_tree_and_insert t left_rb left_idx
    else S.insert_rb_tree t left_idx left_rb;
    if S.Rbtree.S.size right_rb > 2 * t.int_size then
      split_rb_tree_and_insert t right_rb right_idx
    else S.insert_rb_tree t right_idx right_rb

  let repair_t (t: S.t) dt =
    let to_split =
      Hashtbl.fold
        (fun i rb l ->
          if S.Rbtree.S.size rb > 2 * t.int_size
          then i :: l else l)
        t.rb_layer [] in
    List.iter
      (fun i ->
        let rb = Hashtbl.find t.rb_layer i in
        S.remove_rb_tree t i;
        split_rb_tree_and_insert t rb i)
      to_split
    
      

  (* let insert_dt (t: S.t) dt arr range =
    for i = fst range to snd range - 1 do
      insert_dt_aux t dt arr.(i)
    done;
    let done_splitting = ref [] in
    let n_remaining = ref 0 in
    let remaining = ref @@ Ints.fold
      (fun idx l ->
        incr n_remaining;
        (idx, Hashtbl.find t.rb_layer idx) :: l)
      dt.rem [] in
    while !n_remaining > 0 do
      n_remaining := 0;
      remaining := List.fold_left
        (fun acc (idx, rb) ->
          let (left_idx, left_rb), (right_idx, right_rb) =
            S.split_rb_tree rb idx in
          let acc =
            if S.Rbtree.S.size left_rb > 2 * t.int_size then begin
              incr n_remaining;
              (left_idx, left_rb) :: acc
            end else begin
              done_splitting := (left_idx, left_rb) :: !done_splitting;
              acc
            end in
          let acc = 
            if S.Rbtree.S.size right_rb > 2 * t.int_size then begin
              incr n_remaining;
              (right_idx, right_rb) :: acc
            end else begin
              done_splitting := (right_idx, right_rb) :: !done_splitting;
              acc
            end in
          acc)
        [] !remaining
    done;
    dt.add <- !done_splitting

  let repair_t (t: S.t) (arr: dt array) =
    Array.iter
      (fun dt ->
        Ints.iter (fun x -> S.remove_rb_tree t x) dt.rem;
        List.iter (fun (idx, rb) -> S.insert_rb_tree t idx rb) dt.add)
      arr *)

end