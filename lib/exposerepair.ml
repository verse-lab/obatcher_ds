[@@@warning "-32-26-27"]

module type Sequential = sig

  (* Types for sequential implementation *)
  type kt       (* Key type *)
  (* Declare type in Prebatch type signature *)
  type t

  val init : int -> t

  val mem : t -> kt -> bool

  val insert : t -> kt -> unit

  val delete : t -> kt -> unit

  val predecessor : t -> kt -> kt option

  val successor : t -> kt -> kt option

end

module type Prebatch = sig
  
  module S : Sequential

  type dt (* Supplementary data for the exposed data structure *)

  val compare : S.kt -> S.kt -> int
  (** Should expose the comparison function for the ordered key type. *)

  val expose : S.t -> S.kt array -> S.kt array * dt

  val insert_sub_batch : S.t -> S.kt array -> dt -> int * int -> unit

  val repair : S.t -> dt -> unit

end

module Make (P : Prebatch) = struct
  module S = P.S

  type t = S.t

  (* 'a represents the return type of the callback functions *)
  type 'a op =
  | Insert : S.kt -> unit op
  | Member : S.kt -> bool op

  type wrapped_op = Mk : 'a op * ('a -> unit) -> wrapped_op

  let universe_size = ref 24
  let insert_op_threshold = ref 1000
  let search_op_threshold = ref 1000
  (* let size_factor_threshold = ref 5 *)

  let init () = S.init !universe_size

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

  let par_insert ~pool t ~inserts =
    let n = Array.length inserts in
    if n <= 0 then ()
    else if n <= !insert_op_threshold then
      Array.iter (fun x -> S.insert t x) inserts
    else begin
      let num_par = n / !insert_op_threshold + if n mod !insert_op_threshold > 0 then 1 else 0 in
      let pivot_seeds = Array.init num_par (fun i -> inserts.(i)) in (* Assume insert operations are random *)
      Sort.sort pool ~compare pivot_seeds;
      let (pivots_arr, dt) = P.expose t pivot_seeds in
      let npivots = Array.make (Array.length pivots_arr) 0 in
      partition_par pool npivots inserts pivots_arr 0 n;
      let ranges = Array.init
        (Array.length pivots_arr + 1)
        (fun i ->
          if i = 0 then (0, npivots.(i))
          else if i = Array.length pivots_arr then (npivots.(i - 1), n)
          else (npivots.(i - 1), npivots.(i))) in
      Domainslib.Task.parallel_for pool
        ~start:0 ~finish:(Array.length ranges - 1) ~chunk_size:1
        ~body:(fun i -> P.insert_sub_batch t inserts dt ranges.(i));
      P.repair t dt
    end

  let run t (pool: Domainslib.Task.pool) (ops: wrapped_op array) : unit =
    let inserts: S.kt list ref = ref [] in
    let searches: (S.kt * (bool -> unit)) list ref = ref [] in
    Array.iter (function
        | Mk (Member vl, kont) -> searches := (vl,kont) :: !searches
        | Mk (Insert vl, kont) -> kont (); inserts := vl :: !inserts
      ) ops;
    (* now, do all searches in parallel *)
    let searches = Array.of_list !searches in
    Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length searches - 1)
      ~body:(fun i ->
          let key, kont = searches.(i) in
          let result = S.mem t key in
          kont result
        );
    (* now, all inserts *)
    let inserts = Array.of_list !inserts in
    par_insert ~pool t ~inserts

end