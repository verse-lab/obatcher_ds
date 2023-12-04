(* Generalised module type and functors for batching data structures with the
   following pattern and assumptions:
   - Is a key value store where each key is unique and associated with one value.
   - The key must be a total order.
   - Each node can have zero, one, or multiple sorted key value pairs.
   - Each node can have zero, one, or multiple sorted children. (specify)
   - Each node must be a root node of a valid sub-data structure.
   - The top level node must be wrapped in a `'a t` wrapper to allow for
   in-place data structure updates.
  
   Other details are left to the users' discretion, such as:
   - Whether inserting an existing key updates the value or does nothing.
   - Whether the node has an empty `Leaf` variant.
   - The minimum and/or maximum number of children per node.
   - Any further conditions on joinable/splittable trees.
   - Any further metadata to be stored in nodes or the wrapper. *)

module type Sequential = sig

  (* Types for sequential implementation *)
  type kt       (* Key type *)
  type 'a node  (* 'a is the polymorphic value type *)
  type 'a t

  val init : unit -> 'a t

  val search : kt -> 'a t -> 'a option

  val insert : kt -> 'a -> 'a t -> unit

  val delete : kt -> 'a t -> unit

end

module type Prebatch = sig

  module S : Sequential

  val compare : S.kt -> S.kt -> int
  (** Should expose the comparison function for the ordered key type. *)

  val peek_root : 'a S.t -> 'a S.node
  (** [peek_root t] allows access to the root node of the data structure. Does not
      remove the root node from the in-place wrapper. *)

  val set_root : 'a S.node -> 'a S.t -> unit
  (** [set_root node t] sets the root node of the wrapper to [node]. *)

  val size_factor : 'a S.node -> int
  (** Should return a nonnegative integer that is some sort of factor related
      to the size of the data structure that can be used to compare against the
      batch sequential threshold (e.g. height of an AVL tree, black height of a
      Red-Black tree, etc.) *)

  val join : 'a S.t array -> 'a S.t
  (** [join arr] joins all data structures in [arr] together. [arr] must be sorted and
      the data structures in [arr] must not have overlapping ranges of keys. *)

  val split : S.kt array -> 'a S.t -> 'a S.t array
  (** [split arr t] splits [t] into an array of disjoint data structures. For each
      key [i] in sorted [arr], we would have 2 data structures where one has all its node
      keys less than [i] and the other has all its node keys equal or greater than [i].
      The final number of data structures returned should be [Array.length arr + 1],
      including empty ones where necessary, and in the same order as [arr].
      Updates to any resulting data structure must NOT modify the others. *)


  (* The functions below require an extra condition: the number of children in a node
     cannot exceed the number of keys in the node, plus one. Each child must have a
     key range delimited by the keys in the parent node (a b-tree basically). *)

  val break_node : 'a S.node -> (S.kt * 'a) array * 'a S.t array

  (* Functions for optimizing batched search operations, since we don't need
     to modify the tree. *)

  val peek_node : 'a S.node -> S.kt array * 'a S.node array

  val search_node : S.kt -> 'a S.node -> 'a option
  (** Like the Sequential search function, but takes a node as input instead.
      Recommended to simply implement this as an auxiliary search function in
      the Sequential module itself and then binding that to this function. *)

end

module Make (P : Prebatch) = struct

  module S = P.S
  type 'a t = 'a S.t
  type 'a node = 'a S.node
  type ('a, 'b) op =
    | Insert : S.kt * 'a -> ('a, unit) op
    | Search : S.kt -> ('a, 'a option) op
    (* | Delete : S.kt -> ('a, unit) op *)
  type 'a wrapped_op = Mk : ('a, 'b) op * ('b -> unit) -> 'a wrapped_op

  let insert_op_threshold = ref 1000
  let search_op_threshold = ref 50
  let size_factor_threshold = ref 5
  let search_type = ref 1
  let insert_type = ref 0

  let compare = P.compare
  (* let compare (a: S.kt) (b: S.kt) = if a < b then -1 else if a > b then 1 else 0 *)

  let init () = S.init ()

  (** Helper binary search function. Returns the target index or the index of the first
      element greater than the target. *)
  let binary_search arr target left right =
    let left = ref left and right = ref right in
    let mid = ref @@ (!left + !right) / 2 in
    let found = ref false in
    while !left <= !right && not !found do
      mid := (!left + !right) / 2;
      let ck = fst arr.(!mid) in
      if ck = target then found := true
      else if ck < target then left := !mid + 1
      else right := !mid - 1
    done;
    if !found then !mid
    else if fst arr.(!left) >= target then !left
    else 0

  (** Naive batched search operations. We simply split the search operations
      array into equal sub-arrays, and process each sub-array in parallel by
      calling the search function for each search operation at the beginning
      of the linked list. *)
  let rec par_search_aux_1 threshold pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n > threshold then
      let num_par = n / threshold + if n mod threshold > 0 then 1 else 0 in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(num_par - 1) ~chunk_size:1
        ~body:(fun i -> par_search_aux_1 threshold pool node ~keys
          ~range:(rstart + i * threshold, min rstop @@ rstart + (i + 1) * threshold));
    else for i = rstart to rstop - 1 do let (k, kont) = keys.(i) in kont @@ P.search_node k node done

  (** Batched search operations with linear search in sorted search operations array. *)
  let rec par_search_aux_2 op_threshold size_factor_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if P.size_factor node <= size_factor_threshold || n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ P.search_node k node done 
    else
      let nkeys, nodes = P.peek_node node in
      let ptr = ref rstart in
      let ranges = ref [] in
      let start = ref rstart in
      for i = 0 to Array.length nkeys - 1 do
        while !ptr < rstop && fst keys.(!ptr) < nkeys.(i) do ptr := !ptr + 1 done;
        ranges := (!start, !ptr) :: !ranges;
        if !ptr < rstop && nkeys.(i) == fst keys.(!ptr) then
          (snd keys.(!ptr) @@ P.search_node nkeys.(i) node; ptr := !ptr + 1);
        start := !ptr
      done; ranges := (!start, rstop) :: !ranges;
      let ranges_arr = Array.of_list !ranges in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges_arr - 1) ~chunk_size:1
        ~body:(fun i -> let (rstart, rstop) = ranges_arr.(i) in
          par_search_aux_2 op_threshold size_factor_threshold ~pool nodes.(i) ~keys ~range:(rstart, rstop))

  (** Batched search operations with binary search in sorted ops array. *)
  let rec par_search_aux_3 op_threshold size_factor_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if P.size_factor node <= size_factor_threshold || n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ P.search_node k node done 
    else
      let nkeys, nodes = P.peek_node node in
      let last_split = ref rstart in
      let ranges = ref [] in
      for i = 0 to Array.length nkeys - 1 do
        let split = binary_search keys nkeys.(i) !last_split rstop in
        ranges := (!last_split, split) :: !ranges;
        if split < rstop && nkeys.(i) == fst keys.(split) then
          (snd keys.(split) @@ P.search_node nkeys.(i) node; last_split := split + 1)
        else last_split := split
      done; ranges := (!last_split, rstop) :: !ranges;
      let ranges_arr = Array.of_list !ranges in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges_arr - 1) ~chunk_size:1
        ~body:(fun i -> let (rstart, rstop) = ranges_arr.(i) in
          par_search_aux_3 op_threshold size_factor_threshold ~pool nodes.(i) ~keys ~range:(rstart, rstop))

  let par_search ?search_threshold ?tree_threshold ~pool (t: 'a t) keys =
    let search_threshold = match search_threshold with Some t -> t | None -> !search_op_threshold in
    let tree_threshold = match tree_threshold with Some t -> t | None -> !size_factor_threshold in
    match !search_type with
    | 0 -> Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length keys - 1) ~body:(fun i ->
      let (k,kont) = keys.(i) in
      kont @@ S.search k t)
    | 1 -> par_search_aux_1 search_threshold pool (P.peek_root t) ~keys ~range:(0, Array.length keys)
    | 2 -> par_search_aux_2 search_threshold tree_threshold ~pool (P.peek_root t) ~keys ~range:(0, Array.length keys)
    | 3 -> par_search_aux_3 search_threshold tree_threshold ~pool (P.peek_root t) ~keys ~range:(0, Array.length keys)
    | _ -> failwith "Invalid search type"

  (** Split the operations array into sub-arrays containing [] *)
  let par_insert_aux_0 op_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k, v) = inserts.(i) in S.insert k v t done
    else
      let split_ranges = ref [] and split_points = ref [] and start = ref rstart in
      let prev_split = ref rstart in
      while !start < rstop do
        let split = !prev_split + op_threshold in
        if split < rstop then split_ranges := (!prev_split, split) :: !split_ranges;
        split_points := fst inserts.(split) :: !split_points;
        start := split
      done; let split_points_arr = Array.of_list (List.rev !split_points) in
      let split_ranges_arr = Array.of_list (List.rev !split_ranges) in
      let ts = P.split split_points_arr t in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length split_ranges_arr - 1) ~chunk_size:1
        ~body:(fun i -> let (rstart, rstop) = split_ranges_arr.(i) in
          for j = rstart to rstop - 1 do let (k, v) = inserts.(j) in S.insert k v ts.(i) done)

  (** Take a peek at the keys at the root node, and split the operations array
      accordingly before breaking up the node into its children and sending the
      operations sub-arrays down. *)
  let rec par_insert_aux_1 op_threshold size_factor_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || P.size_factor (P.peek_root t) <= size_factor_threshold then
      for i = rstart to rstop - 1 do let (k, v) = inserts.(i) in S.insert k v t done
    else begin
      let (kv_arr, t_arr) = P.break_node (P.peek_root t) in
      let ranges = ref [] and prev_ptr = ref rstart and ptr = ref rstart in
      for i = 0 to Array.length kv_arr - 1 do
        while !ptr < rstop && fst inserts.(!ptr) < fst kv_arr.(i) do ptr := !ptr + 1 done;
        ranges := (!prev_ptr, !ptr) :: !ranges;
        if fst inserts.(!ptr) = fst kv_arr.(i) then (ptr := !ptr + 1);
        prev_ptr := !ptr
      done; ranges := (!prev_ptr, rstop) :: !ranges;
      let ranges_arr = Array.of_list (List.rev !ranges) in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges_arr - 1) ~chunk_size:1
        ~body:(fun i -> let (rstart, rstop) = ranges_arr.(i) in begin
          par_insert_aux_1 op_threshold size_factor_threshold ~pool t_arr.(i) ~inserts ~range:(rstart, rstop);
          if i > 0 then S.insert (fst kv_arr.(i - 1)) (snd kv_arr.(i - 1)) t_arr.(i)
        end);
      P.set_root (P.peek_root @@ P.join t_arr) t
    end

  let rec par_insert_aux_2 op_threshold size_factor_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || P.size_factor (P.peek_root t) <= size_factor_threshold then
      for i = rstart to rstop - 1 do let (k, v) = inserts.(i) in S.insert k v t done
    else begin
      let (kv_arr, t_arr) = P.break_node (P.peek_root t) in
      let ranges = ref [] and prev_ptr = ref rstart in
      for i = 0 to Array.length kv_arr - 1 do
        let ptr = binary_search inserts (fst kv_arr.(i)) !prev_ptr rstop in
        ranges := (!prev_ptr, ptr) :: !ranges;
        if fst inserts.(ptr) = fst kv_arr.(i) then (prev_ptr := ptr + 1)
        else prev_ptr := ptr
      done; ranges := (!prev_ptr, rstop) :: !ranges;
      let ranges_arr = Array.of_list (List.rev !ranges) in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges_arr - 1) ~chunk_size:1
        ~body:(fun i -> let (rstart, rstop) = ranges_arr.(i) in begin
          par_insert_aux_2 op_threshold size_factor_threshold ~pool t_arr.(i) ~inserts ~range:(rstart, rstop);
          if i > 0 then S.insert (fst kv_arr.(i - 1)) (snd kv_arr.(i - 1)) t_arr.(i)
        end);
      P.set_root (P.peek_root @@ P.join t_arr) t
    end

  let par_insert ?insert_threshold ?size_factor_threshold_opt ~pool (t: 'a t) inserts =
    let insert_threshold = match insert_threshold with Some t -> t | None -> !insert_op_threshold in
    let size_factor_threshold = match size_factor_threshold_opt with Some t -> t | None -> !size_factor_threshold in
    match !insert_type with
    | 0 -> par_insert_aux_0 insert_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 1 -> par_insert_aux_1 insert_threshold size_factor_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 2 -> par_insert_aux_2 insert_threshold size_factor_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | _ -> failwith "Invalid insert type"

  let deduplicate ops: (S.kt * 'a) array =
    let new_ops_list = ref [] in
    for i = Array.length ops - 1 downto 0 do
      if i = 0 then new_ops_list := ops.(i) :: !new_ops_list
      else if fst ops.(i) <> fst ops.(i - 1) then new_ops_list := ops.(i) :: !new_ops_list
    done; Array.of_list !new_ops_list

  let run (type a) (t: a t) (pool: Domainslib.Task.pool) (ops: a wrapped_op array) =
    let searches: (S.kt * (a option -> unit)) list ref = ref [] in
    let inserts: (S.kt * a) list ref = ref [] in
    Array.iter (fun (elt: a wrapped_op) -> match elt with
    | Mk (Insert (key, vl), kont) -> kont (); inserts := (key,vl) :: !inserts
    | Mk (Search key, kont) -> searches := (key, kont) :: !searches
    ) ops;

    (* Initiate parallel searches *)
    let searches = Array.of_list !searches in
    if Array.length searches > 0 then
      Sort.sort pool ~compare:(fun (k, _) (k', _) -> compare k k') searches;
      par_search ~pool t @@ deduplicate searches;

    (* Initiate parallel inserts *)
    let inserts = Array.of_list !inserts in
    if Array.length inserts > 0 then begin
      Sort.sort pool ~compare:(fun (k, _) (k', _) -> compare k k') inserts;
      par_insert ~pool t @@ deduplicate inserts;
    end
end