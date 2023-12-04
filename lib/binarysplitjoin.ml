(* Extensible data structure? *)

module type Sequential = sig

  (* Types for sequential implementation *)
  type kt  (* Key type *)
  type 'a node
  type 'a tree

  val new_tree : unit -> 'a tree

  val search : kt -> 'a tree -> 'a option (**)

  val insert : kt -> 'a -> 'a tree -> unit

  val delete : kt -> 'a tree -> unit

end

module type Prebatch = sig

  module S : Sequential

  val key : 'a S.node -> S.kt

  val height : 'a S.node -> int

  val peek_tree : 'a S.tree -> 'a S.node

  val set_root_node : 'a S.node -> 'a S.tree -> unit

  (* join list of trees? *)
  val join : 'a S.tree -> 'a S.node -> 'a S.tree -> 'a S.tree

  val split : 'a S.tree -> S.kt -> 'a S.tree * 'a S.node * 'a S.tree

  (* Functions for optimizing batched search operations, since we don't need
     to modify the tree. *)
     
  val peek_node : 'a S.node -> 'a S.node * 'a S.node

  val search_node : S.kt -> 'a S.node -> 'a option
  (** Like the Sequential search function, but takes a node as input instead.
      Recommended to simply implement this as an auxiliary search function in
      the Sequential module itself and then binding that to this function. *)

end

module Make (P : Prebatch) = struct

  module S = P.S
  type 'a t = 'a S.tree
  type 'a node = 'a S.node
  type ('a, 'b) op =
    | Insert : S.kt * 'a -> ('a, unit) op
    | Search : S.kt -> ('a, 'a option) op
    (* | Delete : S.kt -> ('a, unit) op *)
  type 'a wrapped_op = Mk : ('a, 'b) op * ('b -> unit) -> 'a wrapped_op

  let insert_op_threshold = ref 1000
  let search_op_threshold = ref 50
  let height_threshold = ref 10
  let search_type = ref 1
  let insert_type = ref 0

  let compare (a: S.kt) (b: S.kt) = if a < b then -1 else if a > b then 1 else 0

  let init () = S.new_tree ()

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

  let rec par_search_aux_1 threshold pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n >= threshold then
      let num_par = n / threshold + if n mod threshold > 0 then 1 else 0 in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(num_par - 1) ~chunk_size:1
        ~body:(fun i -> par_search_aux_1 threshold pool node ~keys
          ~range:(rstart + i * threshold, min rstop @@ rstart + (i + 1) * threshold));
    else for i = rstart to rstop - 1 do let (k, kont) = keys.(i) in kont @@ P.search_node k node done

  (** Batched search operations with linear search in sorted ops array. *)
  let rec par_search_aux_2 op_threshold height_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if P.height node < height_threshold || n < op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ P.search_node k node done 
    else
      let k = P.key node in
      let split = ref rstart in
      while !split < rstop && fst keys.(!split) < k do split := !split + 1 done;
      if !split < rstop && k == fst keys.(!split) then begin
        snd keys.(!split) @@ P.search_node k node; split := !split + 1;
      end; let (l, r) = P.peek_node node in
      let l = Domainslib.Task.async pool 
        (fun () -> par_search_aux_2 op_threshold height_threshold ~pool l ~keys ~range:(rstart, !split)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_2 op_threshold height_threshold ~pool r ~keys ~range:(!split, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r

  (** Batched search operations with binary search in sorted ops array. *)
  let rec par_search_aux_3 op_threshold height_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if P.height node < height_threshold || n < op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ P.search_node k node done 
    else
      let k = P.key node in
      let split = ref @@ binary_search keys k rstart rstop in
      if !split < rstop && k == fst keys.(!split) then begin
        snd keys.(!split) @@ P.search_node k node; split := !split + 1;
      end; let (l, r) = P.peek_node node in
      let l = Domainslib.Task.async pool 
        (fun () -> par_search_aux_3 op_threshold height_threshold ~pool l ~keys ~range:(rstart, !split)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_3 op_threshold height_threshold ~pool r ~keys ~range:(!split, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r

  let par_search ?search_threshold ?tree_threshold ~pool (t: 'a t) keys =
    let search_threshold = match search_threshold with Some t -> t | None -> !search_op_threshold in
    let tree_threshold = match tree_threshold with Some t -> t | None -> !height_threshold in
    match !search_type with
    | 0 -> Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length keys - 1) ~body:(fun i ->
      let (k,kont) = keys.(i) in
      kont @@ S.search k t)
    | 1 -> par_search_aux_1 search_threshold pool (P.peek_tree t) ~keys ~range:(0, Array.length keys)
    | 2 -> par_search_aux_2 search_threshold tree_threshold ~pool (P.peek_tree t) ~keys ~range:(0, Array.length keys)
    | 3 -> par_search_aux_3 search_threshold tree_threshold ~pool (P.peek_tree t) ~keys ~range:(0, Array.length keys)
    | _ -> failwith "Invalid search type"

  let rec par_insert_aux_0 op_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k, v) = inserts.(i) in S.insert k v t done
    else
      let mid = rstart + n / 2 in
      let (mk, mv) = inserts.(mid) in
      let (lt, mn, rt) = P.split t mk in
      let nt = S.new_tree () in P.set_root_node mn nt; S.insert mk mv nt;
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux_0 op_threshold ~pool lt ~inserts ~range:(rstart, mid)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_0 op_threshold ~pool rt ~inserts ~range:(mid + 1, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      P.set_root_node (P.peek_tree @@ P.join lt (P.peek_tree nt) rt) t

  let rec par_insert_aux_1 op_threshold height_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || P.height (P.peek_tree t) <= height_threshold then
      for i = rstart to rstop - 1 do let (k, v) = inserts.(i) in S.insert k v t done
    else begin
      let (lt, mn, rt) = P.split t (P.key @@ P.peek_tree t) in
      let split1 = ref rstart in
      while !split1 < rstop && fst inserts.(!split1) < P.key mn do split1 := !split1 + 1 done;
      let split2 = if !split1 < rstop && fst inserts.(!split1) = P.key mn then !split1 + 1 else !split1 in
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux_1 op_threshold height_threshold ~pool lt ~inserts ~range:(rstart, !split1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_1 op_threshold height_threshold ~pool rt ~inserts ~range:(split2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      P.set_root_node (P.peek_tree @@ P.join lt mn rt) t
    end

  let rec par_insert_aux_2 op_threshold height_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || P.height (P.peek_tree t) <= height_threshold then
      for i = rstart to rstop - 1 do let (k, v) = inserts.(i) in S.insert k v t done
    else begin
      let (lt, mn, rt) = P.split t (P.key @@ P.peek_tree t) in
      let split1 = ref @@ binary_search inserts (P.key mn) rstart rstop in
      let split2 = ref !split1 in
      if !split2 < rstop && fst inserts.(!split2) = P.key mn then split2 := !split2 + 1;
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux_2 op_threshold height_threshold ~pool lt ~inserts ~range:(rstart, !split1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_2 op_threshold height_threshold ~pool rt ~inserts ~range:(!split2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      P.set_root_node (P.peek_tree @@ P.join lt mn rt) t
    end

  let par_insert ?insert_threshold ?tree_threshold ~pool (t: 'a t) inserts =
    let insert_threshold = match insert_threshold with Some t -> t | None -> !insert_op_threshold in
    let tree_threshold = match tree_threshold with Some t -> t | None -> !height_threshold in
    match !insert_type with
    | 0 -> par_insert_aux_0 insert_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 1 -> par_insert_aux_1 insert_threshold tree_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 2 -> par_insert_aux_2 insert_threshold tree_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
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
      (* par_insert ~pool t inserts; *)
      par_insert ~pool t @@ deduplicate inserts;
    end
end


(** Remove adjacent repeated operations, defined as operations with the same key. *)
  (* let deduplicate (type a) (ops: a wrapped_op array) =
    let new_ops_list = ref [] in
    for i = 0 to Array.length ops - 1 do
      if i >= Array.length ops - 1 then new_ops_list := ops.(i) :: !new_ops_list
      else begin
        let (elt: a wrapped_op) = ops.(i) in
        let (elt': a wrapped_op) = ops.(i + 1) in
        match elt, elt' with
        | Mk (Insert (key, _), _), Mk (Insert (key', _), _) ->
          if key = key' then () else new_ops_list := ops.(i) :: !new_ops_list
        | Mk (Search key, _), Mk (Search key', _) ->
          if key = key' then () else new_ops_list := ops.(i) :: !new_ops_list
        | _ -> new_ops_list := ops.(i) :: !new_ops_list
      end
    done; Array.of_list !new_ops_list *)

  (* module S = P.S

  type 'a t = 'a S.tree

  type 'a node = 'a S.node

  type ('a, 'b) op =
    | Insert : S.kt * 'a -> ('a, unit) op
    | Search : S.kt -> ('a, 'a option) op

  type 'a wrapped_op = Mk : ('a, 'b) op * ('b -> unit) -> 'a wrapped_op

  let init () = S.new_tree ()

  let deduplicate (type a) (ops: a wrapped_op array) =
    let module M = Map.Make(struct type t = S.kt let compare = S.compare end) in
    let map = ref M.empty in
    let ops = Array.filter (fun (elt: a wrapped_op) -> match elt with
    | Mk (Insert (key, _), _) -> if M.mem key !map then false else (map := M.add key (); true)
    | Mk (Search key, _) -> if M.mem key !map then false else (map := M.add key (); true)
    ) ops in
    ops

  let rec par_search_aux_1 op_threshold height_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if S.is_leaf node then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ P.search_node k node done 
    else
      (* De duplication function *)
      let (l, r) = P.peek_node node in
      let k = P.key *)

(* module type S = sig

  module Sequential : Sequential
  
  type kt (* Key type *)

  type 'a node

  type 'a t

  (* Put under the functor *)
  val insert_op_threshold : int ref
  val search_op_threshold : int ref
  val height_threshold : int ref
  val search_type : int ref
  val insert_type : int ref



  val key : 'a node -> kt

  val compare : kt -> kt -> int

  (* val value : 'a node -> 'a *)

  (* val num_nodes : 'a t -> int *)

  val height : 'a t -> int

  (* val is_leaf : 'a node -> bool *)

  (* val is_empty : 'a t -> bool *)

  (* val wrap_node : 'a node -> 'a t *)

  val unwrap_tree : 'a t -> 'a node

  val set_root_node : 'a node -> 'a t -> unit


  val new_tree : unit -> 'a t

  val search : kt -> 'a t -> 'a option

  val insert : kt -> 'a -> 'a t -> unit

  val delete : kt -> 'a t -> unit


  val peek : 'a node -> 'a node * 'a node

  val expose : 'a node -> 'a node * 'a node * 'a node

  val join : 'a t -> 'a node -> 'a t -> 'a t

  val split : 'a t -> kt -> 'a t * 'a node * 'a t

end *)