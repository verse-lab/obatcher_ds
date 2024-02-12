(* van Emde Boas tree, but lazy *)

module Sequential = struct
  type kt = Int

  type t = {
    mutable uni_size: int;
    mutable min: int option;
    mutable max: int option;
    mutable summary: t option;
    mutable clusters: t option array;
  }

  let upper_sqrt n = 1 lsl ((log (float_of_int n) /. log 2.) /. 2. |> ceil |> int_of_float)

  let lower_sqrt n = 1 lsl ((log (float_of_int n) /. log 2.) /. 2. |> floor |> int_of_float)

  let check_size n = n == 1 lsl ((log (float_of_int n) /. log 2.) |> floor |> int_of_float)

  let high t x = x / lower_sqrt t.uni_size

  let low t x = x mod lower_sqrt t.uni_size

  let index t x y = x * lower_sqrt t.uni_size + y

  let get_min t = t.min

  let get_max t = t.max

  let create universe =
    if not (check_size universe) then
      failwith "universe size must be a power of 2";
    { uni_size = universe; min = None; max = None; summary = None; clusters = Array.make (upper_sqrt universe) None }

  let rec mem t x =
    if t.min = Some x || t.max = Some x then true
    else if t.uni_size = 2 then false
    else
      let c = t.clusters.(high t x) in
      if c = None then false
      else mem (Option.get c) (low t x)

  let rec successor t x =
    if t.uni_size == 2 then
      if x == 0 && t.max = Some 1 then Some 1 else None
    else if t.min != None && x < Option.get t.min then
      Some (Option.get t.min)
    else
      let max_low = Option.bind t.clusters.(high t x) get_max in
      if max_low != None && low t x < Option.get max_low then
        let offset = successor (Option.get t.clusters.(high t x)) (low t x) in
        Some (index t (high t x) (Option.get offset))
      else
        let succ_cluster = match t.summary with
          | None -> None
          | Some sc -> successor sc (high t x) in
        if succ_cluster = None then None
        else
          let offset = get_min (Option.get t.clusters.(Option.get succ_cluster)) in
          Some (index t (Option.get succ_cluster) (Option.get offset))

  let rec predecessor t x =
    if t.uni_size == 2 then
      if x == 1 && t.min = Some 0 then Some 0 else None
    else if t.max != None && x > Option.get t.max then
      Some (Option.get t.max)
    else
      let min_low = Option.bind t.clusters.(high t x) get_min in
      if min_low != None && low t x > Option.get min_low then
        let offset = predecessor (Option.get t.clusters.(high t x)) (low t x) in
        Some (index t (high t x) (Option.get offset))
      else
        let pred_cluster = match t.summary with
          | None -> None
          | Some sc -> predecessor sc (high t x) in
        if pred_cluster = None then
          if t.min != None && x > Option.get t.min then
            Some (Option.get t.min)
          else None
        else
          let offset = get_max (Option.get t.clusters.(Option.get pred_cluster)) in
          Some (index t (Option.get pred_cluster) (Option.get offset))

  let empty_insert t x = t.min <- Some x; t.max <- Some x

  let rec insert t x = 
    if t.min = None then empty_insert t x
    else begin
      if x = Option.get t.min || x = Option.get t.max then ()
      else begin
        let nx = ref x in
        if x < Option.get t.min then
          (nx := Option.get t.min; t.min <- Some x);
        if t.uni_size > 2 then begin
          if t.clusters.(high t !nx) = None then
            t.clusters.(high t !nx) <- Some (create @@ lower_sqrt t.uni_size);
          let c = Option.get t.clusters.(high t !nx) in
          if get_min c = None then begin
            if t.summary = None then t.summary <- Some (create @@ upper_sqrt t.uni_size);
            insert (Option.get t.summary) (high t !nx);
            empty_insert c (low t !nx)
          end else insert c (low t !nx);
        end;
        if !nx > Option.get t.max then t.max <- Some !nx
      end
    end
      
  let rec delete t x =
    if t.min = None then ()
    else if x < Option.get t.min || x > Option.get t.max then ()
    else if t.min = t.max then begin
      if Option.get t.min <> x then ()
      else (t.min <- None; t.max <- None)
    end else if t.uni_size == 2 then
      (if x == 0 then t.min <- Some 1
      else t.min <- Some 0;
      t.max <- t.min)
    else begin
      let cx = ref x in
      if !cx == Option.get t.min then begin
        let first_cluster_opt = Option.bind t.summary get_min in
        if first_cluster_opt = None then begin
          t.min <- None; t.max <- None;
        end else begin
          let first_cluster = Option.get first_cluster_opt in
          cx := index t first_cluster @@ Option.get @@ get_min @@ Option.get t.clusters.(first_cluster);
          t.min <- Some !cx
        end;
      end;
      (match t.clusters.(high t !cx) with
      | None -> ()
      | Some c -> delete c (low t !cx));
      if Option.bind t.clusters.(high t !cx) get_min = None then begin
        delete (Option.get t.summary) (high t !cx);
        if !cx = Option.get t.max then begin
          let summary_max = get_max (Option.get t.summary) in
          if summary_max = None then t.max <- t.min
          else t.max <- Some (index t (Option.get summary_max) @@ Option.get @@ get_max @@ Option.get t.clusters.(Option.get summary_max))
        end
      end else if !cx = Option.get t.max then
        t.max <- Some (index t (high t !cx) @@ Option.get @@ get_max @@ Option.get t.clusters.(high t !cx))
    end

  (* let flatten t =
    let rec aux t offset (acc: int list): int list =
      let acc =
        if t.uni_size = 2 && t.min != None && Option.get t.min < Option.get t.max then
          offset + Option.get t.max :: acc
        else
          if t.clusters = None
          then acc
          else let cluster_idx = ref (Array.length (Option.get t.clusters)) in
            Array.fold_right
              (fun ct acc ->
                decr cluster_idx;
                aux ct (!cluster_idx * lower_sqrt t.uni_size + offset) acc)
              (Option.get t.clusters)
              acc in
      if t.min = None then acc else offset + Option.get t.min :: acc in
    aux t 0 []
  
  let expose_node t =
    let n_elem = lower_sqrt t.uni_size in
    let pivots = Array.init (upper_sqrt t.uni_size - 1) (fun i -> (i + 1) * n_elem) in
    (pivots, Option.get t.clusters)

  let update_node t =
    let clusters = Option.get t.clusters in

    (* Update summary and find minimum and maximum elements in the cluster *)
    let cmin_idx = ref None and cmax = ref None in
    Array.iteri
      (fun i ct ->
        if ct.min != None then begin
          if !cmin_idx = None then cmin_idx := Some i;
          insert (Option.get t.summary) i;
        end;
        if ct.max != None then cmax := ct.max;)
      clusters;

    (* Replace minimum and maximum in current node if needed *)
    if !cmin_idx != None then begin
      let cmin = Option.get clusters.(Option.get !cmin_idx).min in 
      if t.min = None || cmin <= Option.get t.min then begin
        delete clusters.(Option.get !cmin_idx) cmin;
        let tmp_min = t.min in
        t.min <- Some cmin;
        if tmp_min != None then insert t (Option.get tmp_min)
      end;
      if !cmax != None && (t.max = None || t.max < !cmax)then t.max <- !cmax
    end *)

end

(* module Make = struct
  module Sequential = Sequential
  type t = Sequential.t

  (* 'a represents the return type of the callback functions *)
  type 'a op =
  | Insert : int -> unit op
  | Member : int -> bool op

  type wrapped_op = Mk : 'a op * ('a -> unit) -> wrapped_op

  let init () = Sequential.create @@ 1 lsl 22

  let insert_op_threshold = ref 1000
  let search_op_threshold = ref 1000
  let size_factor_threshold = ref 5
  let search_type = ref 0
  let insert_type = ref 4

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
    else for i = rstart to rstop - 1 do let (k, kont) = keys.(i) in kont @@ Sequential.mem node k done
    
  let par_search ?search_threshold ?tree_threshold ~pool t keys =
    let search_threshold = match search_threshold with Some t -> t | None -> !search_op_threshold in
    let tree_threshold = match tree_threshold with Some t -> t | None -> !size_factor_threshold in
    (match !search_type with
    | 0 | 1 | 2 -> ()
    | _ -> Sort.sort pool ~compare:(fun (k, _) (k', _) -> compare k k') keys);
    match !search_type with
    | 0 -> 
      (* if Array.length keys < search_threshold then
        Array.iter (fun (k, kont) -> kont @@ P.search_node k node) keys
      else *)
        Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length keys - 1) ~body:(fun i ->
          let (k,kont) = keys.(i) in
          kont @@ Sequential.mem t k)
    | 1 -> par_search_aux_1 search_threshold pool t ~keys ~range:(0, Array.length keys)
    (* | 2 -> par_search_aux_2 search_threshold tree_threshold ~pool (P.peek_root t) ~keys ~range:(0, Array.length keys)
    | 3 -> par_search_aux_3 search_threshold tree_threshold ~pool (P.peek_root t) ~keys ~range:(0, Array.length keys) *)
    | _ -> failwith "Invalid search type"

  (** Split the operations array by partitioning the unsorted insert operations until
      threshold. *)
  (* let par_insert_aux_1 op_threshold ~pool t ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do Sequential.insert t inserts.(i) done
    else begin
      let num_par = n / op_threshold + if n mod op_threshold > 0 then 1 else 0 in
      let pivots_arr = Array.init num_par (fun i -> inserts.(i)) in   (* Assume that the inserts are random *)
      Array.sort compare pivots_arr;  (* Single-threaded sorting of pivots *)
      let npivots = Array.make (Array.length pivots_arr) 0 in
      partition_par pool npivots inserts pivots_arr rstart rstop;
      let ranges = Array.init
        (Array.length pivots_arr + 1)
        (fun i ->
          if i = 0 then (rstart, npivots.(i))
          else if i = Array.length pivots_arr then (npivots.(i - 1), rstop)
          else (npivots.(i - 1), npivots.(i))) in
      let ts = P.split pivots_arr t in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges - 1) ~chunk_size:1
        ~body:(fun i -> let (rstart, rstop) = ranges.(i) in
          for j = rstart to rstop - 1 do let (k, v) = inserts.(j) in S.insert k v ts.(i) done);
      P.set_root (P.peek_root @@ P.join ts) t
    end *)

  let rec par_insert_aux_1 op_threshold ~pool t ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold then
      (
        (* Printf.printf "Running sequentially. Inserting %d elements.\n" n; *)
      for i = rstart to rstop - 1 do
        (* Printf.printf "Inserting %d\n" inserts.(i); *)
        Sequential.insert t inserts.(i);
        (* Printf.printf "We did it!\n" *)
      done)
    else begin
      (* Printf.printf "Initialization should not be here\n"; *)
      let (pivots_arr, t_arr) = Sequential.expose_node t in
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
          else (npivots.(i - 1), npivots.(i))) in ()
      (* Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges - 1) ~chunk_size:1
        ~body:(fun i ->
          (* if i > 0 then Sequential.insert (fst kv_arr.(i - 1)) (snd kv_arr.(i - 1)) t_arr.(i); *)
          for j = fst ranges.(i) to snd ranges.(i) - 1 do 
            inserts.(j) <- Sequential.low t inserts.(j)
          done;
          par_insert_aux_1 op_threshold size_factor_threshold ~pool t_arr.(i) ~inserts ~range:ranges.(i));
      Sequential.update_node t *)
    end

  let par_insert_aux_4 op_threshold ~pool t ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || Sequential.uni_size t <= 2 then
      (
        (* Printf.printf "Running sequentially. Inserting %d elements.\n" n; *)
      for i = rstart to rstop - 1 do
        (* Printf.printf "Inserting %d\n" inserts.(i); *)
        Sequential.insert t inserts.(i);
        (* Printf.printf "We did it!\n" *)
      done)
    else begin
      (* Printf.printf "Initialization should not be here\n"; *)
      let (_, t_arr) = Sequential.expose_node t in
      let num_par = 20 in
      (* let num_par = n / op_threshold + if n mod op_threshold > 0 then 1 else 0 in *)
      let num_elem_per_cluster = Sequential.lower_sqrt t.uni_size in
      let pivots_arr = Array.init num_par (fun i -> Sequential.high t inserts.(i) * num_elem_per_cluster) in   (* Assume that the inserts are random *)
      Sort.sort pool ~compare pivots_arr;
      let pivots_arr = deduplicate pivots_arr in
      let npivots = Array.make (Array.length pivots_arr) 0 in
      partition_par pool npivots inserts pivots_arr rstart rstop;

      (* for i = rstart to rstop - 1 do
        Printf.printf "Elem %d: %d\n" i inserts.(i)
      done;
      for i = 0 to Array.length pivots_arr - 1 do
        Printf.printf "Pivot %d: %d\n" i pivots_arr.(i)
      done; *)
      (* let pivots_arr = Array.init (Array.length kv_arr) (fun i -> fst kv_arr.(i)) in *)
      (* let pivots_arr = Array.init (Array.length kv_arr) (fun i -> fst kv_arr.(i)) in *)
      (* let npivots = Array.make (Array.length pivots_arr) 0 in *)
      (* Printf.printf "Sorting %d elements...\n" (Array.length inserts); *)
      (* Sort.sort pool ~compare inserts; *)
      (* partition_par pool npivots inserts pivots_arr rstart rstop; *)
      (* Printf.printf "Partitioned with %d pivots...\n" (Array.length pivots_arr); *)
      let ranges = Array.init
        (Array.length pivots_arr + 1)
        (fun i ->
          if i = 0 then (rstart, npivots.(i))
          else if i = Array.length pivots_arr then (npivots.(i - 1), rstop)
          else (npivots.(i - 1), npivots.(i))) in
      let clusters = Sequential.clusters t in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length ranges - 1) ~chunk_size:1
        ~body:(fun i ->
          for j = fst ranges.(i) to snd ranges.(i) - 1 do 
            Sequential.insert
              clusters.(Sequential.high t inserts.(j)) @@ Sequential.low t inserts.(j)
          done);
      Sequential.update_node t
    end

  let par_insert ?insert_threshold ?size_factor_threshold_opt ~pool t inserts =
    let insert_threshold = match insert_threshold with Some t -> t | None -> !insert_op_threshold in
    let size_factor_threshold = match size_factor_threshold_opt with Some t -> t | None -> !size_factor_threshold in
    (match !insert_type with
    | 2 | 3 -> Sort.sort pool ~compare inserts
    | _ -> ());
    match !insert_type with
    (* | 0 -> par_insert_aux_0 insert_threshold ~pool t ~inserts:inserts ~range:(0, Array.length inserts) *)
    | 1 -> par_insert_aux_1 insert_threshold ~pool t ~inserts:inserts ~range:(0, Array.length inserts)
    (* | 2 -> par_insert_aux_2 insert_threshold size_factor_threshold ~pool t ~inserts:(deduplicate inserts) ~range:(0, Array.length inserts) *)
    (* | 3 -> par_insert_aux_3 insert_threshold size_factor_threshold ~pool t ~inserts:(deduplicate inserts) ~range:(0, Array.length inserts) *)
    | 4 -> par_insert_aux_4 insert_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
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