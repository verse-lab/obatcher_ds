(** Basic ordered linked list. *)

[@@@warning "-32-26-27"]

module Sequential (V: Map.OrderedType) = struct
  type kt = V.t
  (* We keep track of the last node to speed up the join operation later.
     The [Hd] variant doubles as the in-place "wrapper" for the linked list. *)
  type 'a node =
  | Hd of {next: 'a node ref; last: 'a node ref}
  | Null
  | Node of {
      key: kt;
      value: 'a;
      next: 'a node ref;
    }
  type 'a t = 'a node

  let init () = Hd {next = ref Null; last = ref Null}

  let rec size = function
  | Null -> 0 | Hd {next; _} -> size !next
  | Node {next; _} -> 1 + size !next

  let key = function
    | Hd _ | Null -> failwith "can't extract key from Hd or Null"
    | Node {key; _} -> key

  let set_last n t = match t with
    | Hd {last; _} -> last := n
    | _ -> failwith "Need head of linked list to set last node"

  let rec find_last_node (n: 'a node) = match n with
  | Hd {next; _} | Node {next; _} -> if !next = Null then n else find_last_node !next
  | Null -> Null

  let new_node k v = Node {key = k; value = v; next = ref Null}

  let num_nodes t =
    let rec aux n = function
    | Null -> n | Hd {next; _} -> aux n !next
    | Node {next; _} -> aux (n + 1) !next
    in aux 0 t

  let flatten t =
    let rec aux acc = function
    | Null -> acc | Hd {next; _} -> aux acc !next
    | Node {key; value; next} -> aux ((key, value) :: acc) !next
    in List.rev @@ aux [] t

  let rec search k n =
    match n with
    | Null -> None | Hd {next; _} -> search k !next
    | Node {key; value; next} ->
      if V.compare k key = 0 then Some value
      else search k !next

  let rec insert_aux k v n t =
    match n with
    | Null -> ()
    | Hd {next; _} | Node {next; _} ->
      match !next with
      | Hd _ -> failwith "Hd variant in middle of linked list"
      | Null ->
        let new_node = Node {key = k; value = v; next = ref Null} in
        next := new_node; set_last new_node t
      | Node {key; _} ->
        if V.compare k key = 0 then ()
        else if V.compare k key > 0 then insert_aux k v !next t
        else next := Node {key = k; value = v; next = ref !next}

  let insert k v t = insert_aux k v t t

  let rec delete_aux k n t =
    match n with
    | Null -> ()
    | Hd {next; _} | Node {next; _} ->
      match !next with
      | Hd _ -> failwith "Hd variant in middle of linked list"
      | Null -> ()
      | Node {key; next = next_next; _} ->
        if V.compare k key = 0 then (if !next = find_last_node t then set_last n t; next := !next_next)
        else if V.compare k key > 0 then delete_aux k !next t
        else ()

  let delete k t = delete_aux k t t

end

module Prebatch (V: Map.OrderedType)  = struct
  
  module S = Sequential(V)
  let key = S.key
  let compare = V.compare
  let size_factor = S.size

  let peek_root (t: 'a S.t) = match t with
  | Hd {next; _} -> !next | _ -> failwith "Invalid head"

  let find_last_node = S.find_last_node

  let set_root (n: 'a S.node) (t: 'a S.t) = match t with
  | Hd {next; last} -> begin
    match n with
    | Hd {next = n'; last = ln'} -> (next := !n'; last := !ln')
    | _ -> (next := n; last := find_last_node n)
    end
  | _ -> failwith "invalid start of linked list"

  let break_node (n: 'a S.node) = match n with
  | Node {key; value; next} ->
    ([|key, value|], [|S.Hd {next = next; last = ref (find_last_node n)}|])
  | _ -> failwith "invalid node"

  let join_two (lt: 'a S.t) (rt: 'a S.t) =
    match lt, rt with
    | Hd {last = llast; _}, Hd {next = rnext; last = rlast} -> begin
      match !llast with
      | Null -> rt
      | Node {next; _} -> (next := !rnext; llast := !rlast; lt)
      | _ -> failwith "Invalid last position for right list"
      end
    | _ -> failwith "Both lists must start with Hd"

  let join (t_arr: 'a S.t array) =
    let acc = ref t_arr.(0) in
    for i = 0 to Array.length t_arr - 2 do
      acc := join_two !acc t_arr.(i + 1);
    done; !acc

  (* let join_two (lt: 'a S.t) (n: 'a S.node) (rt: 'a S.t) =
    match n with
    | Node {next = nnext; _} -> begin
      match lt, rt with
      | Hd {last = llast; _}, Hd {next = rnext; _} -> begin
        match !llast with
        | Null -> (nnext := !rnext; rnext := n; rt)
        | Node {next; _} -> (next := n; llast := n; nnext := !rnext; lt)
        | _ -> failwith "undefined"
        end
      | _ -> failwith "undefined"
      end
    | _ -> begin
      match lt, rt with
      | Hd {last = llast; _}, Hd {next = rnext; last = rlast} -> begin
        match !llast with
        | Null -> rt
        | Node {next; _} -> (next := !rnext; llast := !rlast; lt)
        | _ -> failwith "undefined"
        end
      | _ -> failwith "undefined"
      end

  let join (n_arr: 'a S.node array) (t_arr: 'a S.t array) =
    assert (Array.length n_arr + 1 = Array.length t_arr);
    let n_ptr = ref 0 in
    let acc = ref t_arr.(0) in
    for i = 0 to Array.length t_arr - 2 do
      acc := join_two !acc n_arr.(!n_ptr) t_arr.(i + 1);
      n_ptr := !n_ptr + 1
    done; !acc *)

  let rec split_two k (n: 'a S.node) (t: 'a S.t) = 
    match n with
    | Null -> failwith "n argument is Null"
    | Hd {next = next_n; _} | Node {next = next_n; _} ->
      match !next_n with
      | Hd _ -> failwith "Hd variant in middle of linked list"
      | Null -> (t, S.init ())
      | Node {key = next_key; next = next_next_n; _} ->
        if V.compare k next_key <= 0 then begin
          let last = find_last_node t in
          S.set_last n t;
          let (_, next_arr) = break_node n in let next_n = next_arr.(0) in
          (t, Hd {next = ref next_n; last = ref last})
          (* if V.compare k next_key = 0 then begin
            let (_, next_arr) = break_node n in let next_n = next_arr.(0) in
            let (_, next_arr) = break_node next_n in let next_next_n = next_arr.(0) in
            (t, next_n, Hd {next = ref next_next_n; last = ref last})
          end
          else begin
            let (_, next_arr) = break_node n in let next_n = next_arr.(0) in
            (t, S.Null, Hd {next = ref next_n; last = ref last})
          end *)
        end
      else split_two k !next_n t

  let split k_arr t =
    let acc = ref t in
    let t_arr = Array.make (Array.length k_arr + 1) S.Null in
    for i = 0 to Array.length k_arr - 1 do
      let (lt, rt) = split_two k_arr.(i) !acc !acc in
      acc := rt; t_arr.(i) <- lt
    done; t_arr.(Array.length k_arr) <- !acc; t_arr

  let peek_node (n: 'a S.node) = match n with
  | Null -> [||], [||], [|S.Null|]
  | Hd {next; _} -> [||], [||], [|!next|]
  | Node {next; value; key} -> [|key|], [|value|], [|!next|]

  let search_node k n = S.search k n
end