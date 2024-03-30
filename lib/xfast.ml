[@@@warning "-32-26-39-27"]

module type XfastTabl = sig

  type 'a t

  val create : int -> 'a t

  val find_in_level : 'a t -> int -> int -> 'a option

  val set_at_level : 'a t -> int -> int -> 'a -> unit

  val remove_from_level : 'a t -> int -> int -> unit

  val previous_at_level : 'a t -> int -> int -> int

  val flatten : 'a t -> int list

end

(* Array-based static hash table for batched Xfast trie *)
module ArrXfastTabl = struct

  type 'a t = {
    max_lvl : int;
    arr : 'a option array array
  }

  let create n_levels = {
    max_lvl = n_levels - 1;
    arr = Array.init n_levels (fun i -> Array.make (Int.shift_left 1 (i + 1)) None)
  }

  let get_pos t lvl x =
    if lvl < 0 || lvl > t.max_lvl then failwith "Invalid level for trie";
    Int.shift_right_logical x (t.max_lvl - lvl)

  let find_in_level t lvl x = 
    let pos = get_pos t lvl x in
    if pos >= 0 && pos < Array.length t.arr.(lvl) then
      t.arr.(lvl).(get_pos t lvl x)
    else None

  let set_at_level t lvl x n = t.arr.(lvl).(get_pos t lvl x) <- Some n

  let remove_from_level t lvl x = t.arr.(lvl).(get_pos t lvl x) <- None

  let previous_at_level t lvl x =
    if lvl < 0 || lvl > t.max_lvl then failwith "Invalid level for trie";
    let idx = Int.shift_right_logical x (t.max_lvl - lvl) in
    Int.shift_left (idx - 1) (t.max_lvl - lvl)

  let flatten t =
    let arr = t.arr.(t.max_lvl) in
    let l = ref [] in
    for i = Array.length arr - 1 downto 0 do
      if arr.(i) != None then l := i :: !l
    done; !l

end

module Sequential (T : XfastTabl) = struct

  module T = T

  type kt = int

  type node = {
    mutable lvl: int; (* Root level should be zero. *)
    mutable value: int option; (* Should it contain the prefix at each level? Should only be None for root. *)
    mutable parent: node option;
    mutable has_left: bool;
    mutable left: node option;
    mutable has_right: bool;
    mutable right: node option;
  }

  type t = {
    max_lvl: int;
    max_val: int;
    tables: node T.t;
    mutable root: node;
    (* mutable min: node option;
    mutable max: node option; *)
  }

  let get_value n = n.value

  (* max_size should be the number of bits of max integer. Defaults to Sys.int_size - 1,
    the maximum possible bit length for nonnegative numbers. *)
  let init int_size =
    (* let int_size =
      match int_size with
      | None -> Sys.int_size - 1 (* Note that Sys.int_size = 63, minus one (62) to account for sign bit. *)
      | Some i -> *)
    if int_size > 0 && int_size <= Sys.int_size - 1 then () else failwith "Invalid int size";
    {
      max_lvl = int_size - 1;
      max_val = Int.shift_right_logical Int.max_int (Sys.int_size - 1 - int_size);
      tables = T.create int_size;
      root = {
        lvl = -1;
        value = None;
        parent = None;
        has_left = false;
        left = None;
        has_right = false;
        right = None
      };
      (* min = None;
      max = None *)
    }

  let mem t x =
    match T.find_in_level t.tables t.max_lvl x with
    | None -> false
    | Some _ -> true

  (* This is log(u) complexity *)
  (* let longest_prefix_node t x =
    let rec aux n =
      let pos = Int.shift_left 1 (t.max_lvl - n.lvl) in
      let is_right = Int.logand x pos > 0 in
      if is_right then
        if not n.has_right then n else aux (Option.get n.right)
      else if not n.has_left then n else aux (Option.get n.left) in
    aux t.root *)

  let get_prefix_at_lvl t lvl x =
    let mask = Int.logxor t.max_val (Int.shift_right_logical t.max_val (lvl + 1)) in
    Int.logand mask x

  (* Helper function to find the minimum node starting at some node n while ignoring threaded links. *)
  let rec get_min_node n =
    if n.has_left then
      get_min_node (Option.get n.left)
    else if n.has_right then
      get_min_node (Option.get n.right)
    else n

  (* Helper function to find the maximum node starting at some node n. *)
  let rec get_max_node n =
    if n.has_right then
      get_max_node (Option.get n.right)
    else if n.has_left then
      get_max_node (Option.get n.left)
    else n

  let longest_prefix_node_from_lvl t start_node start_lvl x =
    let low = ref start_lvl in
    let high = ref @@ t.max_lvl in
    let res_node = ref start_node in
    while !low <= !high do
      let mid = (!low + !high) / 2 in
      let prefix = get_prefix_at_lvl t mid x in
      (* let mask = Int.logxor t.max_val (Int.shift_right_logical t.max_val (mid + 1)) in
      let prefix = Int.logand mask x in *)
      let mid_node = T.find_in_level t.tables mid prefix in
      if mid_node != None then begin
        res_node := Option.get mid_node;
        low := mid + 1
      end else high := mid - 1

    done; !res_node

  let longest_prefix_node t x = longest_prefix_node_from_lvl t t.root 0 x

  (* let nearest_node_from_prefix t n x =
    if n.lvl = t.max_lvl then Some n else begin
    (* Printf.printf "Hi\n"; *)
    (* if n.value != None && n.lvl = t.max_lvl && Option.get n.value = x then Some n else begin *)
      (* Printf.printf "Sup\n"; *)
      let pos = Int.shift_left 1 (t.max_lvl - (n.lvl + 1)) in
      let is_right = Int.logand x pos > 0 in
      let descendant = if is_right then n.right else n.left in
      if descendant != None then begin
        let descendant = Option.get descendant in
        let candidate = if is_right then descendant.right else descendant.left in
        if candidate = None then
          Some descendant
        else
          let candidate = Option.get candidate in
          if abs ((Option.get descendant.value) - x) < abs((Option.get candidate.value) - x) then
            Some descendant
          else Some candidate
      end else None
    end *)

  (* let successor_node_from_prefix t n x =
    if Option.get n.value = x then
      n.right
    else begin
      let pos = Int.shift_left 1 (t.max_lvl - (n.lvl + 1)) in
      let is_right = Int.logand x pos > 0 in
      let descendant = if is_right then n.right else n.left in
      if descendant != None then begin
        let descendant = Option.get descendant in
        let candidate = if is_right then descendant.right else descendant.left in
        if candidate = None 
      end;
      n.right
    end *)

  (* let successor_from_prefix t n x =
    Option.bind
      (nearest_node_from_prefix t n x)
      (fun n' -> if Option.get n'.value > x then Some n' else n'.right)

  let predecessor_from_prefix t n x =
    Option.bind
      (nearest_node_from_prefix t n x)
      (fun n' -> if Option.get n'.value < x then Some n' else n'.left) *)

  (* let successor_from_prefix t prefix_node x =
    if prefix_node.lvl = t.max_lvl || not prefix_node.has_right then
      prefix_node.right
    else begin
      let pos = Int.shift_left 1 (t.max_lvl - (prefix_node.lvl + 1)) in
      let is_right = Int.logand x pos > 0 in
      let next_node = if is_right then prefix_node.left else prefix_node.right in
      let res = Option.bind
        next_node
        (fun n' -> if Option.get n'.value <= x then n'.right else next_node) in
      assert (if res = None then true else (Option.get res).lvl = t.max_lvl); res
    end

  let predecessor_from_prefix t prefix_node x =
    if prefix_node.lvl = t.max_lvl || not prefix_node.has_left then
      prefix_node.left
    else begin
      Printf.printf "Level: %d\n" prefix_node.lvl;
      let pos = Int.shift_left 1 (t.max_lvl - (prefix_node.lvl + 1)) in
      let is_right = Int.logand x pos > 0 in
      let next_node = if is_right then prefix_node.left else prefix_node.right in
      let res = Option.bind
        next_node
        (fun n' -> if Option.get n'.value >= x then n'.left else next_node) in
      assert (if res = None then true else (Option.get res).lvl = t.max_lvl); res
    end *)

  let successor_from_prefix _t n x =
    if not n.has_right then
      n.right
    else if n.left <> None then
      (Option.get n.left).right
    else
      Some (get_min_node @@ Option.get n.right)

  let predecessor_from_prefix _t n x =
    if not n.has_left then
      n.left
    else if n.right <> None then
      (Option.get n.right).left
    else
      Some (get_max_node @@ Option.get n.left)

  let successor_node t x =
    match T.find_in_level t.tables t.max_lvl x with
    | Some n' -> n'.right
    | None -> successor_from_prefix t (longest_prefix_node t x) x

  let predecessor_node t x =
    match T.find_in_level t.tables t.max_lvl x with
    | Some n' -> n'.left
    | None -> predecessor_from_prefix t (longest_prefix_node t x) x

  let successor t x = Option.bind (successor_node t x) get_value

  let predecessor t x = Option.bind (predecessor_node t x) get_value

  let rec insert_node_aux t n x xp xs =
    if n.lvl = t.max_lvl then begin
      n.left <- xp; n.right <- xs;
      T.set_at_level t.tables n.lvl x n; n
    end else
      let pos = Int.shift_left 1 (t.max_lvl - (n.lvl + 1)) in
      let is_right = Int.logand x pos > 0 in
      (* let mask = Int.logxor t.max_val (Int.shift_right_logical t.max_val (n.lvl + 2)) in
      let prefix = Int.logand mask x in *)
      let prefix = get_prefix_at_lvl t (n.lvl + 1) x in
      if (is_right && not n.has_right) || ((not is_right) && not n.has_left) then begin
        let new_node = {
          lvl = n.lvl + 1;
          value = Some prefix;
          parent = Some n;
          has_left = false;
          left = None;
          has_right = false;
          right = None
        } in
        T.set_at_level t.tables (n.lvl + 1) prefix new_node;
        if is_right then begin
          n.has_right <- true;
          n.right <- Some new_node;
          if not n.has_left then n.left <- xp
        end else begin
          n.has_left <- true;
          n.left <- Some new_node;
          if not n.has_right then n.right <- xs
        end;
        insert_node_aux t new_node x xp xs
      end else if is_right then
        insert_node_aux t (Option.get n.right) x xp xs
      else insert_node_aux t (Option.get n.left) x xp xs

  let insert_node t n x =
    if mem t x then () else begin
      (* Printf.printf "wee\n"; *)
      let prefix_node = longest_prefix_node_from_lvl t n (n.lvl + 1) x in
      let xs = successor_from_prefix t prefix_node x in
      let xp = predecessor_from_prefix t prefix_node x in
      let new_node = insert_node_aux t prefix_node x xp xs in
      (* if x > Option.get (Option.get t.max).value then
        t.max <- Some new_node;
      if x < Option.get (Option.get t.min).value then
        t.min <- Some new_node;
      assert (new_node.lvl = t.max_lvl); *)
      (* Printf.printf "Yay\n"; *)
      if xs <> None then begin
        let cn = ref (new_node) in
        let cs = ref (Option.get xs) in
        while !cs != !cn  do
          if not !cs.has_left then !cs.left <- Some new_node;
          cs := Option.get !cs.parent;
          cn := Option.get !cn.parent;
        done
      end;
      if xp <> None then
        let cn = ref (new_node) in
        let cp = ref (Option.get xp) in
        while !cp != !cn do
          if not !cp.has_right then !cp.right <- Some new_node;
          cp := Option.get !cp.parent;
          cn := Option.get !cn.parent;
        done;
    end

  let insert t x = insert_node t t.root x

  let delete t x =
    if not (mem t x) then () else begin
      let xnode = Option.get @@ T.find_in_level t.tables t.max_lvl x in
      let xp = xnode.left and xs = xnode.right in
      (* if x = Option.get (Option.get t.max).value then
        t.max <- xp;
      if x = Option.get (Option.get t.min).value then
        t.min <- xs; *)
      let rec delete_aux n rem =
        if n == t.root then () else begin
          let parent = Option.get n.parent in
          if rem then begin
            let pos = Int.shift_left 1 (t.max_lvl - n.lvl) in
            let is_right = Int.logand x pos > 0 in
            (* let mask = Int.logxor t.max_val (Int.shift_right_logical t.max_val (n.lvl + 1)) in
            let prefix = Int.logand mask x in *)
            let prefix = get_prefix_at_lvl t n.lvl x in
            T.remove_from_level t.tables n.lvl prefix;
            if is_right then (parent.has_right <- false; parent.right <- xs)
            else (parent.has_left <- false; parent.left <- xp);
          end;
          if not (parent.has_right || parent.has_left) then
            delete_aux parent true
          else
            delete_aux parent false
        end in
      delete_aux xnode true;
      if xs <> None then begin
        let cs = ref (Option.get xs) in
        while !cs != t.root  do
          if (not !cs.has_left) && !cs.left != None && Option.get !cs.left == xnode then
            !cs.left <- xp;
          cs := Option.get !cs.parent;
        done
      end;
      if xp <> None then begin
        let cp = ref (Option.get xp) in
        while !cp != t.root do
          if (not !cp.has_right) && !cp.right != None && Option.get !cp.right == xnode then
            !cp.right <- xs;
          cp := Option.get !cp.parent;
        done
      end
    end

  let rec update_max_ptr n mp =
    if n.has_right then
      update_max_ptr (Option.get n.right) mp
    else begin
      n.right <- mp;
      if n.has_left then
        update_max_ptr (Option.get n.left) mp
    end

  let rec update_min_ptr n mp =
    if n.has_left then
      update_min_ptr (Option.get n.left) mp
    else begin
      n.left <- mp;
      if n.has_right then
        update_min_ptr (Option.get n.right) mp
    end

  (* Find the pivot, which is just the integer value of the prefix. *)
  (* We assume there is no connecting *)
  let expose_node t n =
    let left_prefix =
      if n.value = None then 0 else Option.get n.value in
    let right_prefix =
      left_prefix + Int.shift_left 1 (t.max_lvl - (n.lvl + 1)) in
    let left_node =
      if not n.has_left then begin
        let new_left = {
          lvl = n.lvl + 1;
          value = Some left_prefix;
          parent = Some n;
          has_left = false;
          left = None;
          has_right = false;
          right = None
        } in
        T.set_at_level t.tables (n.lvl + 1) left_prefix new_left;
        n.left <- Some new_left; new_left
      end else Option.get n.left in
    let right_node =
      if not n.has_right then begin
        let new_right = {
          lvl = n.lvl + 1;
          value = Some right_prefix;
          parent = Some n;
          has_left = false;
          left = None;
          has_right = false;
          right = None
        } in
        T.set_at_level t.tables (n.lvl + 1) right_prefix new_right;
        n.right <- Some new_right; new_right
      end else Option.get n.right in
    update_max_ptr left_node None;
    update_min_ptr right_node None;
    ([|right_prefix|], [|left_node; right_node|])

  let flatten t = T.flatten t.tables

end

module Prebatch (T : XfastTabl) = struct
  module S = Sequential(T)

  type dt = int

  let compare = Int.compare

  let deduplicate ops: S.kt array =
    let new_ops_list = ref [] in
    for i = Array.length ops - 1 downto 0 do
      if i = 0 || ops.(i) <> ops.(i - 1)
      then new_ops_list := ops.(i) :: !new_ops_list
    done; Array.of_list !new_ops_list

  (* let make_pivots (t: S.t) arr =
    let f_plength = float_of_int @@ Array.length arr in
    let target_lvl = int_of_float (ceil @@ log f_plength /. log 2.) - 1 in
    deduplicate @@ Array.init
      (Array.length arr)
      (fun i -> S.get_prefix_at_lvl t target_lvl arr.(i)) *)

  let update_node (t: S.t) (n: S.node) =
    let right_node = Option.get n.right in
    let left_node = Option.get n.left in
    if not (right_node.has_left || right_node.has_right) then begin
      S.T.remove_from_level t.tables right_node.lvl @@
        Option.get right_node.value;
      n.right <- None
    end;
    if not (left_node.has_left || left_node.has_right) then begin
      S.T.remove_from_level t.tables left_node.lvl @@
        Option.get left_node.value;
      n.left <- None
    end;
    if n.right != None then
      S.update_min_ptr right_node (Option.bind n.left (fun n' -> Some (S.get_max_node n')));
    if n.left != None then
      S.update_max_ptr left_node (Option.bind n.right (fun n' -> Some (S.get_min_node n')))

  let rec init_to_level_aux (t: S.t) (n: S.node) lvl =
    if n.lvl = lvl then () else begin
      let left_prefix =
        if n.value = None then 0 else Option.get n.value in
      if not n.has_left then begin
        n.left <- Some {
          lvl = n.lvl + 1;
          value = Some left_prefix;
          parent = Some n;
          has_left = false;
          left = None;
          has_right = false;
          right = None
        };
        T.set_at_level t.tables (n.lvl + 1) left_prefix (Option.get n.left);
      end;
      if not n.has_right then begin
        let right_prefix =
          left_prefix + Int.shift_left 1 (t.max_lvl - (n.lvl + 1)) in
        n.right <- Some {
          lvl = n.lvl + 1;
          value = Some right_prefix;
          parent = Some n;
          has_left = false;
          left = None;
          has_right = false;
          right = None
        };
        T.set_at_level t.tables (n.lvl + 1) right_prefix (Option.get n.right);
      end;
      n.has_right <- true;
      n.has_left <- true;
      init_to_level_aux t (Option.get n.left) lvl;
      init_to_level_aux t (Option.get n.right) lvl;
    end

  let init_to_level t lvl = init_to_level_aux t t.root lvl

  (* let expose_node (t: S.t) (n: S.node) =
    let left_prefix =
      if n.value = None then 0 else Option.get n.value in
    let right_prefix =
      left_prefix + Int.shift_left 1 (t.max_lvl - (n.lvl + 1)) in
    let left_node =
      if not n.has_left then begin
        let new_left = {
          S.lvl = n.lvl + 1;
          value = Some left_prefix;
          parent = Some n;
          has_left = false;
          left = None;
          has_right = false;
          right = None
        } in
        T.set_at_level t.tables (n.lvl + 1) left_prefix new_left;
        n.left <- Some new_left; new_left
      end else Option.get n.left in
    let right_node =
      if not n.has_right then begin
        let new_right = {
          S.lvl = n.lvl + 1;
          value = Some right_prefix;
          parent = Some n;
          has_left = false;
          left = None;
          has_right = false;
          right = None
        } in
        T.set_at_level t.tables (n.lvl + 1) right_prefix new_right;
        n.right <- Some new_right; new_right
      end else Option.get n.right in
    S.update_max_ptr left_node None;
    S.update_min_ptr right_node None *)

  let expose_t (t: S.t) arr =
    let f_plength = float_of_int @@ Array.length arr in
    let target_lvl = int_of_float (ceil @@ log f_plength /. log 2.) - 1 in
    let pivots = deduplicate @@ Array.init
      (Array.length arr)
      (fun i -> S.get_prefix_at_lvl t target_lvl arr.(i)) in
    init_to_level t target_lvl;
    Array.iter
      (fun pivot ->
        let node = Option.get @@ S.T.find_in_level t.tables target_lvl pivot in
        S.update_min_ptr node None;
        let prev_node_idx = S.T.previous_at_level t.tables target_lvl pivot in
        let prev_node = S.T.find_in_level t.tables target_lvl prev_node_idx in
          (* S.T.previous_at_level t.tables target_lvl pivot
          |> S.T.find_in_level t.tables target_lvl in *)
        if prev_node != None then
          S.update_max_ptr (Option.get prev_node) None
        )
      pivots;
    (pivots, target_lvl)

  let insert_t (t: S.t) arr dt range =
    for i = fst range to snd range - 1 do
      (* let mask = Int.logxor t.max_val (Int.shift_right_logical t.max_val (dt + 1)) in
      let prefix = Int.logand mask arr.(i) in *)
      let prefix = S.get_prefix_at_lvl t dt arr.(i) in
      let entry_node = Option.get @@ T.find_in_level t.tables dt prefix in
      S.insert_node t entry_node arr.(i)
    done

  let rec repair_t_aux (t: S.t) (n: S.node) target_lvl =
    if n.lvl < target_lvl then begin
      if n.has_right then
        repair_t_aux t (Option.get n.right) target_lvl;
      if n.has_left then
        repair_t_aux t (Option.get n.left) target_lvl;
    end; update_node t n

  let repair_t (t: S.t) dt =
    repair_t_aux t t.root dt

end

(* module Make = struct
  module Sequential = Sequential(ArrXfastTabl)
  type t = Sequential.t
  type node = Sequential.node

  (* 'a represents the return type of the callback functions *)
  type 'a op =
  | Insert : int -> unit op
  | Member : int -> bool op

  type wrapped_op = Mk : 'a op * ('a -> unit) -> wrapped_op

  let init = Sequential.init ~int_size:24

  let insert_op_threshold = ref 1000
  let search_op_threshold = ref 1000
  let size_factor_threshold = ref 5
  let search_type = ref 0
  let insert_type = ref 1

  let compare = Int.compare

  (** Helper binary search function. Returns the target index or the index of the first
      element greater than the target. *)
  let binary_search arr target left right =
    let left = ref left and right = ref right in
    let mid = ref @@ (!left + !right) / 2 in
    let found = ref false in
    while !left <= !right && not !found do
      mid := (!left + !right) / 2;
      let ck = arr.(!mid) in
      if ck = target then found := true
      else if ck < target then left := !mid + 1
      else right := !mid - 1
    done;
    if !found then !mid
    else if arr.(!left) >= target then !left
    else 0

  (** Helper function to remove duplicated insert operations (since effectively only one
      can take effect at the end, in any order thanks to linearisation). *)
  let deduplicate ops: 'a array =
    let new_ops_list = ref [] in
    for i = Array.length ops - 1 downto 0 do
      if i = 0 || ops.(i) <> ops.(i - 1)
      then new_ops_list := ops.(i) :: !new_ops_list
    done; Array.of_list !new_ops_list

  (** Helper function to partition an array of operations. Basically the QuickSort
      partition function, except that the "pivot" is provided as an argument *)
  let partition_two arr pivot lo hi =
    if hi <= lo then failwith "Invalid partition range"
    else
      let i = ref lo in
      for j = lo to hi - 1 do
        if arr.(j) < pivot then begin
          let tmp = arr.(!i) in
          arr.(!i) <- arr.(j);
          arr.(j) <- tmp;
          i := !i + 1
        end
      done;
      if arr.(!i) < pivot then !i + 1 else !i

  (** Partition a list of operations given an array of pivots. Returns a list of
      indices to separate the partitions. *)
  let partition_seq res_list arr pivot_list lo hi =
    (* Slower version; simply iterates through each pivot in order *)
    (* let clo = ref lo in 
    Array.iteri (fun i p -> res_list.(i) <- partition_two arr p !clo hi; clo := res_list.(i)) pivot_list *)
    let rec aux pstart pstop lo hi =
      if pstop - pstart <= 0 then ()
      else if pstop - pstart = 1 then res_list.(pstart) <- partition_two arr pivot_list.(pstart) lo hi
      else
        let pmid = pstart + (pstop - pstart) / 2 in
        res_list.(pmid) <- partition_two arr pivot_list.(pmid) lo hi;
        aux pstart pmid lo res_list.(pmid);
        aux (pmid + 1) pstop res_list.(pmid) hi in
    aux 0 (Array.length pivot_list) lo hi

  let partition_par pool res_list arr pivot_list lo hi =
    let rec aux pstart pstop lo hi =
      if pstop - pstart <= 0 then ()
      else if pstop - pstart = 1 then res_list.(pstart) <- partition_two arr pivot_list.(pstart) lo hi
      else
        let pmid = pstart + (pstop - pstart) / 2 in
        res_list.(pmid) <- partition_two arr pivot_list.(pmid) lo hi;
        let l = Domainslib.Task.async pool (fun () -> aux pstart pmid lo res_list.(pmid)) in
        let r = Domainslib.Task.async pool (fun () -> aux (pmid + 1) pstop res_list.(pmid) hi) in
        Domainslib.Task.await pool l; Domainslib.Task.await pool r in
    aux 0 (Array.length pivot_list) lo hi

  let par_search ?search_threshold ?tree_threshold ~pool t keys =
    let search_threshold = match search_threshold with Some t -> t | None -> !search_op_threshold in
    let tree_threshold = match tree_threshold with Some t -> t | None -> !size_factor_threshold in
    (* (match !search_type with
    | 0 | 1 | 2 -> ()
    | _ -> Sort.sort pool ~compare:(fun (k, _) (k', _) -> compare k k') keys); *)
    match !search_type with
    | 0 -> 
      (* if Array.length keys < search_threshold then
        Array.iter (fun (k, kont) -> kont @@ P.search_node k node) keys
      else *)
        Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length keys - 1) ~body:(fun i ->
          let (k,kont) = keys.(i) in
          kont @@ Sequential.mem t k)
    (* | 1 -> par_search_aux_1 search_threshold pool t ~keys ~range:(0, Array.length keys) *)
    (* | 2 -> par_search_aux_2 search_threshold tree_threshold ~pool (P.peek_root t) ~keys ~range:(0, Array.length keys)
    | 3 -> par_search_aux_3 search_threshold tree_threshold ~pool (P.peek_root t) ~keys ~range:(0, Array.length keys) *)
    | _ -> failwith "Invalid search type"

  let rec par_insert_aux_1 op_threshold ~pool t node ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold then
      (
        (* Printf.printf "Running sequentially. Inserting %d elements.\n" n; *)
      for i = rstart to rstop - 1 do
        (* Printf.printf "Inserting %d\n" inserts.(i); *)
        Sequential.insert_node t node inserts.(i);
        (* Printf.printf "We did it!\n" *)
      done)
    else begin
      (* Printf.printf "Initialization should not be here\n"; *)
      let (pivots_arr, t_arr) = Sequential.expose_node t node in
      (* for i = rstart to rstop - 1 do
        Printf.printf "Elem %d: %d\n" i inserts.(i)
      done;
      for i = 0 to Array.length pivots_arr - 1 do
        Printf.printf "Pivot %d: %d\n" i pivots_arr.(i)
      done; *)
      (* let pivots_arr = Array.init (Array.length kv_arr) (fun i -> fst kv_arr.(i)) in *)
      (* let pivots_arr = Array.init (Array.length kv_arr) (fun i -> fst kv_arr.(i)) in *)
      let npivots = Array.make (Array.length pivots_arr) 0 in
      (* Printf.printf "Sorting %d elements...\n" (Array.length inserts); *)
      (* Sort.sort pool ~compare inserts; *)
      partition_par pool npivots inserts pivots_arr rstart rstop;
      (* Printf.printf "Partitioned with %d pivots...\n" (Array.length pivots_arr); *)
      let ranges = Array.init
        (Array.length pivots_arr + 1)
        (fun i ->
          if i = 0 then (rstart, npivots.(i))
          else if i = Array.length pivots_arr then (npivots.(i - 1), rstop)
          else (npivots.(i - 1), npivots.(i))) in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges - 1) ~chunk_size:1
        ~body:(fun i ->
          (* if i > 0 then Sequential.insert (fst kv_arr.(i - 1)) (snd kv_arr.(i - 1)) t_arr.(i); *)
          (* for j = fst ranges.(i) to snd ranges.(i) - 1 do 
            inserts.(j) <- Sequential.low t inserts.(j)
          done; *)
          par_insert_aux_1 op_threshold ~pool t t_arr.(i) ~inserts ~range:ranges.(i));
      (* Sequential.update_node t node *)
    end

  let par_insert ?insert_threshold ?size_factor_threshold_opt ~pool t inserts =
    let insert_threshold = match insert_threshold with Some t -> t | None -> !insert_op_threshold in
    let size_factor_threshold = match size_factor_threshold_opt with Some t -> t | None -> !size_factor_threshold in
    (match !insert_type with
    | 2 | 3 -> Sort.sort pool ~compare inserts
    | _ -> ());
    match !insert_type with
    (* | 0 -> par_insert_aux_0 insert_threshold ~pool t ~inserts:inserts ~range:(0, Array.length inserts) *)
    | 1 -> par_insert_aux_1 insert_threshold ~pool t t.root ~inserts:inserts ~range:(0, Array.length inserts)
    (* | 2 -> par_insert_aux_2 insert_threshold size_factor_threshold ~pool t ~inserts:(deduplicate inserts) ~range:(0, Array.length inserts) *)
    (* | 3 -> par_insert_aux_3 insert_threshold size_factor_threshold ~pool t ~inserts:(deduplicate inserts) ~range:(0, Array.length inserts) *)
    (* | 4 -> par_insert_aux_4 insert_threshold ~pool t ~inserts ~range:(0, Array.length inserts) *)
    | _ -> failwith "Invalid insert type"

  let run t (pool: Domainslib.Task.pool) (ops: wrapped_op array) : unit =
    let inserts: int list ref = ref [] in
    let searches: (int * (bool -> unit)) list ref = ref [] in
    (* let size = lazy (Sequential.size t) in *)
    Array.iter (function
        (* | Mk (Size, kont) -> kont (Lazy.force size) *)
        | Mk (Member vl, kont) -> searches := (vl,kont) :: !searches
        | Mk (Insert vl, kont) -> kont (); inserts := vl :: !inserts
      ) ops;
    (* now, do all searches in parallel *)
    let searches = Array.of_list !searches in
    Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length searches - 1)
      ~body:(fun i ->
          let key, kont = searches.(i) in
          let result = Sequential.mem t key in
          kont result
        );
    (* now, all inserts *)
    let inserts = Array.of_list !inserts in
    (* Printf.printf "Inserting batched now with %d elements.\n" (Array.length inserts); *)
    par_insert ~pool t inserts
end *)
