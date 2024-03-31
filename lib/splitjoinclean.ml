module type Sequential = sig

  type kt
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

  val set_root : 'a S.t -> 'a S.t -> unit
  (** [set_root t1 t2] sets t1 to be t2 in-place. *)

  val size_factor : 'a S.t -> int
  (** Should return a nonnegative integer that is some sort of factor related
      to the size of the data structure that can be used to compare against the
      batch sequential threshold (e.g. height of an AVL tree, black height of a
      Red-Black tree, etc.) *)

  val join : 'a S.t -> 'a S.t -> 'a S.t
  (** [join arr] joins two data structures together. They must be provided in
      order and must not have overlapping ranges of keys. *)

  val split : 'a S.t -> S.kt -> 'a S.t * 'a S.t
  (** [split t k] splits t into two data structures at [k] *)

end

module Make (P : Prebatch) = struct

  module S = P.S
  type 'a t = 'a S.t
  type ('a, 'b) op =
    | Insert : S.kt * 'a -> ('a, unit) op
    | Search : S.kt -> ('a, 'a option) op
    (* | Delete : S.kt -> ('a, unit) op *)
  type 'a wrapped_op = Mk : ('a, 'b) op * ('b -> unit) -> 'a wrapped_op

  let insert_op_threshold = ref 500
  let search_op_threshold = ref 1000
  let size_factor_threshold = ref 5

  let compare = P.compare

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

  (** Helper function to remove duplicated insert operations (since effectively only one
      can take effect at the end, in any order thanks to linearisation). *)
  let deduplicate ops: (S.kt * 'a) array =
    let new_ops_list = ref [] in
    for i = Array.length ops - 1 downto 0 do
      if i = 0 || fst ops.(i) <> fst ops.(i - 1)
      then new_ops_list := ops.(i) :: !new_ops_list
    done; Array.of_list !new_ops_list

  (** Helper function to partition an array of operations. Basically the QuickSort
      partition function, except that the "pivot" is provided as an argument *)
  let partition_two arr pivot lo hi =
    if hi <= lo then failwith "Invalid partition range"
    else
      let i = ref lo in
      for j = lo to hi - 1 do
        if fst arr.(j) < pivot then begin
          let tmp = arr.(!i) in
          arr.(!i) <- arr.(j);
          arr.(j) <- tmp;
          i := !i + 1
        end
      done;
      if fst arr.(!i) < pivot then !i + 1 else !i

  (** Partition a list of operations given an array of pivots. Returns a list of
      indices to separate the partitions. *)
  let partition_seq res_list arr pivot_list lo hi =
    let rec aux pstart pstop lo hi =
      if pstop - pstart <= 0 then ()
      else if pstop - pstart = 1 then res_list.(pstart) <- partition_two arr pivot_list.(pstart) lo hi
      else
        let pmid = pstart + (pstop - pstart) / 2 in
        res_list.(pmid) <- partition_two arr pivot_list.(pmid) lo hi;
        aux pstart pmid lo res_list.(pmid);
        aux (pmid + 1) pstop res_list.(pmid) hi in
    aux 0 (Array.length pivot_list) lo hi

  (** Same as [partition_seq], but parallelised. *)
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

  let join (t_arr: 'a S.t array) =
    let acc = ref t_arr.(0) in
    for i = 1 to Array.length t_arr - 1 do
      acc := P.join !acc t_arr.(i);
    done; !acc

  let split k_arr t =
    let acc = ref t and t_list = ref [] in
    for i = Array.length k_arr - 1 downto 0 do
      let (lt, rt) = P.split !acc k_arr.(i) in
      acc := lt; t_list := rt :: !t_list;
    done; Array.of_list @@ !acc :: !t_list

  let par_insert_aux op_threshold tree_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || P.size_factor t <= tree_threshold then
      for i = rstart to rstop - 1 do let (k, v) = inserts.(i) in S.insert k v t done
    else begin
      let num_par = n / op_threshold + if n mod op_threshold > 0 then 1 else 0 in
      let pivots_arr = Array.init num_par (fun i -> fst inserts.(i)) in   (* Assume that the inserts are random *)
      Sort.sort pool ~compare pivots_arr;  (* Single-threaded sorting of pivots *)
      let npivots = Array.make (Array.length pivots_arr) 0 in
      partition_par pool npivots inserts pivots_arr rstart rstop;
      let ranges = Array.init
        (Array.length pivots_arr + 1)
        (fun i ->
          if i = 0 then (rstart, npivots.(i))
          else if i = Array.length pivots_arr then (npivots.(i - 1), rstop)
          else (npivots.(i - 1), npivots.(i))) in
      let ts = split pivots_arr t in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges - 1) ~chunk_size:1
        ~body:(fun i -> let (rstart, rstop) = ranges.(i) in
          for j = rstart to rstop - 1 do let (k, v) = inserts.(j) in S.insert k v ts.(i) done);
      P.set_root t @@ join ts
    end

  let par_insert ?insert_threshold ?size_factor_threshold_opt ~pool (t: 'a t) inserts =
    let insert_threshold = match insert_threshold with Some t -> t | None -> !insert_op_threshold in
    let size_factor_threshold = match size_factor_threshold_opt with Some t -> t | None -> !size_factor_threshold in
    par_insert_aux insert_threshold size_factor_threshold ~pool t ~inserts:inserts ~range:(0, Array.length inserts)

  let par_search ~pool (t: 'a t) searches =
    Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length searches - 1)
      ~body:(fun i ->
        let (k,kont) = searches.(i) in
        kont @@ S.search k t)

  let run (type a) (t: a t) (pool: Domainslib.Task.pool) (ops: a wrapped_op array) =
    (* Printf.printf "Batch size: %d\n" (Array.length ops); *)
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
      par_insert ~pool t inserts
    end

end