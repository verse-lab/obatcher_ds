module type S = sig
  
  type kt (* Key type *)

  type 'a node

  type 'a t

  val insert_op_threshold : int ref

  val search_op_threshold : int ref

  val height_threshold : int ref

  val search_type : int ref

  val insert_type : int ref



  val key : 'a node -> kt

  val compare : kt -> kt -> int

  val value : 'a node -> 'a

  val num_nodes : 'a t -> int

  val flatten : 'a t -> (kt * 'a) list

  val get_height : 'a t -> int

  val is_leaf : 'a node -> bool

  val is_empty : 'a t -> bool

  val wrap_node : 'a node -> 'a t

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

end


module Make (S : S) = struct

  module Sequential = S

  type 'a t = 'a S.t

  type 'a node = 'a S.node

  type ('a, 'b) op =
    | Insert : S.kt * 'a -> ('a, unit) op
    | Search : S.kt -> ('a, 'a option) op

  type 'a wrapped_op = Mk : ('a, 'b) op * ('b -> unit) -> 'a wrapped_op

  let init () = S.new_tree ()

  (** Use linear search only to traverse operations array *)
  let rec par_search_aux_1 op_threshold height_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if S.is_leaf node then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ S.search k (S.wrap_node node) done 
    else
      let (l, r) = S.peek node in
      let k = S.key node in
      let nval = S.value node in
      let s1 = ref rstart and s2 = ref rstart in
      while !s1 < rstop && fst keys.(!s1) < k do s1 := !s1 + 1 done;
      s2 := !s1;
      while !s2 < rstop && fst keys.(!s2) = k do
        snd keys.(!s2) (Some nval);
        s2 := !s2 + 1
      done;
      let l = Domainslib.Task.async pool 
        (fun () -> par_search_aux_1 op_threshold height_threshold ~pool l ~keys ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_1 op_threshold height_threshold ~pool r ~keys ~range:(!s2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r

  let par_search ~pool (t: 'a t) keys =
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> S.compare k k') keys;
    match !S.search_type with
    | 0 -> Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length keys - 1) ~body:(fun i ->
      let (k,kont) = keys.(i) in
      kont @@ S.search k t)
    | 1 -> par_search_aux_1 !S.search_op_threshold !S.height_threshold ~pool (S.unwrap_tree t) ~keys ~range:(0, Array.length keys)
    (* | 2 -> par_search_aux_2 op_threshold tree_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)
    | 3 -> par_search_aux_3 op_threshold tree_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)
    | 4 -> par_search_aux_4 op_threshold tree_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys) *)
    | _ -> failwith "Invalid search type"
 
  (** Linear traversal of inserts *)
  let rec par_insert_aux_1 op_threshold height_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || S.get_height t <= height_threshold then
      for i = rstart to rstop - 1 do
        let (k, v) = inserts.(i) in
        S.insert k v t
      done
    else
      let (ln, mn, rn) = S.expose @@ S.unwrap_tree t in
      let lt = S.wrap_node ln and rt = S.wrap_node rn in
      let mid1 = ref rstart in
      while !mid1 < rstop && fst inserts.(!mid1) < S.key mn do mid1 := !mid1 + 1 done;
      let mid2 = ref !mid1 in
      while !mid2 < rstop && fst inserts.(!mid2) <= S.key mn do mid2 := !mid2 + 1 done;
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux_1 op_threshold height_threshold ~pool lt ~inserts ~range:(rstart, !mid1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_1 op_threshold height_threshold ~pool rt ~inserts ~range:(!mid2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      let nt = S.join lt mn rt in
      S.set_root_node (S.unwrap_tree nt) t

  let par_insert ~pool (t: 'a t) inserts =
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> S.compare k k') inserts;
    match !S.insert_type with
    (* | 0 -> par_insert_aux_0 threshold ~pool t ~inserts ~range:(0, Array.length inserts) *)
    | 1 -> par_insert_aux_1 !S.insert_op_threshold !S.height_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    (* | 2 -> par_insert_aux_2 threshold !avltree_insert_height_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 3 -> par_insert_aux_3 threshold !avltree_insert_height_threshold ~pool t ~inserts ~range:(0, Array.length inserts) *)
    | _ -> failwith "Invalid insert type"

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
      par_search ~pool t searches;

    (* Initiate parallel inserts *)
    let inserts = Array.of_list !inserts in
    if Array.length inserts > 0 then begin
      Sort.sort pool ~compare:(fun (k1,_) (k2,_) -> S.compare k1 k2) inserts;
      par_insert ~pool t inserts;
    end

end