(* van Emde Boas tree *)
[@@@warning "-32-26-39-27"]

module Sequential = struct

  type kt = Int.t

  type t = {
    mutable uni_size: int;
    mutable min: int option;
    mutable max: int option;
    mutable summary: t option;
    mutable clusters: t array option;
  }

  let uni_size t = t.uni_size

  let clusters t = Option.get t.clusters

  let upper_sqrt n = 1 lsl ((log (float_of_int n) /. log 2.) /. 2. |> ceil |> int_of_float)

  let lower_sqrt n = 1 lsl ((log (float_of_int n) /. log 2.) /. 2. |> floor |> int_of_float)

  let check_size n = n == 1 lsl ((log (float_of_int n) /. log 2.) |> floor |> int_of_float)

  let high t x = x / lower_sqrt t.uni_size

  let low t x = x mod lower_sqrt t.uni_size

  let index t x y = x * lower_sqrt t.uni_size + y

  let get_min t = t.min

  let get_max t = t.max

  let rec create universe =
    if not (check_size universe) then
      failwith "universe size must be a power of 2"
    else if universe = 2 then
      { uni_size = universe; min = None; max = None; summary = None; clusters = None }
    else
      let num_clusters = upper_sqrt universe in
      let summary = Some (create num_clusters) in
      let clusters = Some (Array.init num_clusters (fun _ -> create @@ lower_sqrt universe)) in
      {uni_size = universe; min = None; max = None; summary; clusters}

  let init = create

  let rec mem t x =
    if t.min = Some x || t.max = Some x then true
    else if t.uni_size = 2 then false
    else mem (Option.get t.clusters).(high t x) (low t x)

  let rec successor t x =
    if t.uni_size == 2 then
      if x == 0 && t.max = Some 1 then Some 1 else None
    else if t.min != None && x < Option.get t.min then
      Some (Option.get t.min)
    else
      let max_low = get_max (Option.get t.clusters).(high t x) in
      if max_low != None && low t x < Option.get max_low then
        let offset = successor (Option.get t.clusters).(high t x) (low t x) in
        Some (index t (high t x) (Option.get offset))
      else
        let succ_cluster = match t.summary with
          | None -> None
          | Some sc -> successor sc (high t x) in
        if succ_cluster = None then None
        else
          let offset = get_min (Option.get t.clusters).(Option.get succ_cluster) in
          Some (index t (Option.get succ_cluster) (Option.get offset))

  let rec predecessor t x =
    if t.uni_size == 2 then
      if x == 1 && t.min = Some 0 then Some 0 else None
    else if t.max != None && x > Option.get t.max then
      Some (Option.get t.max)
    else
      let min_low = get_min (Option.get t.clusters).(high t x) in
      if min_low != None && low t x > Option.get min_low then
        let offset = predecessor (Option.get t.clusters).(high t x) (low t x) in
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
          let offset = get_max (Option.get t.clusters).(Option.get pred_cluster) in
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
          let c = (Option.get t.clusters).(high t !nx) in
          if get_min c = None then begin
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
          cx := index t first_cluster @@ Option.get @@ get_min @@ (Option.get t.clusters).(first_cluster);
          t.min <- Some !cx
        end;
      end;
      delete (Option.get t.clusters).(high t !cx) (low t !cx);
      if get_min (Option.get t.clusters).(high t !cx) = None then begin
        delete (Option.get t.summary) (high t !cx);
        if !cx = Option.get t.max then begin
          let summary_max = get_max (Option.get t.summary) in
          if summary_max = None then t.max <- t.min
          else t.max <- Some (index t (Option.get summary_max) @@ Option.get @@ get_max (Option.get t.clusters).(Option.get summary_max))
        end
      end else if !cx = Option.get t.max then
        t.max <- Some (index t (high t !cx) @@ Option.get @@ get_max (Option.get t.clusters).(high t !cx))
    end

  let flatten t =
    let rec aux t offset (acc: int list): int list =
      let acc =
        if t.uni_size = 2 && t.min != None && Option.get t.min < Option.get t.max then
          offset + Option.get t.max :: acc
        else
          if t.clusters = None then acc else
            let cluster_idx = ref (Array.length (Option.get t.clusters)) in
            Array.fold_right
              (fun ct acc ->
                decr cluster_idx;
                aux ct (!cluster_idx * lower_sqrt t.uni_size + offset) acc)
              (Option.get t.clusters)
              acc in
      if t.min = None then acc else offset + Option.get t.min :: acc in
    aux t 0 []

end

module Prebatch = struct

  module S = Sequential

  type dt = unit

  let init = S.create

  let compare = Int.compare

  let deduplicate ops: S.kt array =
    let new_ops_list = ref [] in
    for i = Array.length ops - 1 downto 0 do
      if i = 0 || ops.(i) <> ops.(i - 1)
      then new_ops_list := ops.(i) :: !new_ops_list
    done; Array.of_list !new_ops_list

  let make_pivots (t: S.t) (arr: S.kt array) =
    let num_elem_per_cluster = Sequential.lower_sqrt t.uni_size in
    deduplicate @@ Array.init
      (Array.length arr)
      (fun i -> Sequential.high t arr.(i) * num_elem_per_cluster)
    (* for i = 0 to Array.length arr - 1 do
      arr.(i) <- Sequential.high t arr.(i) * num_elem_per_cluster
    done *)

  let expose (t: S.t) (arr: S.kt array) =
    (make_pivots t arr, ())
    (* let n_elem = S.lower_sqrt t.uni_size in
    let pivots = Array.init (S.upper_sqrt t.uni_size - 1) (fun i -> (i + 1) * n_elem) in
    (pivots, Option.get t.clusters) *)

  let insert_sub_batch t arr _dt range =
    let clusters = S.clusters t in
    for i = fst range to snd range - 1 do
      S.insert clusters.(S.high t arr.(i)) (S.low t arr.(i))
    done

  let repair (t: S.t) _dt =
    let clusters = Option.get t.clusters in

    (* Update summary and find minimum and maximum elements in the cluster *)
    let cmin_idx = ref None and cmax_idx = ref None in
    Array.iteri
      (fun i ct ->
        if S.get_min ct = None then
          S.delete (Option.get t.summary) i
        else begin
          S.insert (Option.get t.summary) i; (* Update summary accordingly *)
          if !cmin_idx = None then cmin_idx := Some i;
          cmax_idx := Some i
        end)
      clusters;

    if !cmin_idx != None then begin
      let cmin_idx = Option.get !cmin_idx in
      let cluster_min_low = Option.get @@ clusters.(cmin_idx).min in
      let cluster_min = S.index t cmin_idx cluster_min_low in
      if t.min = None || cluster_min <= Option.get t.min then begin
        S.delete clusters.(cmin_idx) cluster_min_low;
        if clusters.(cmin_idx).min = None then
          S.delete (Option.get t.summary) cmin_idx;
        if t.min = None then begin
          t.min <- Some cluster_min;
          let cmax_idx = Option.get !cmax_idx in
          t.max <- Some (S.index t cmax_idx (Option.get clusters.(cmax_idx).max))
        end else if Option.get t.min > cluster_min then begin
          let tmp = t.min in
          t.min <- Some cluster_min;
          let cmax_idx = Option.get !cmax_idx in
          t.max <- Some (S.index t cmax_idx (Option.get clusters.(cmax_idx).max));
          S.insert t (Option.get tmp)
        end else
          let cmax_idx = Option.get !cmax_idx in
          t.max <- Some (S.index t cmax_idx (Option.get clusters.(cmax_idx).max));
      end
    end

end