[@@@warning "-32-26"]
let treap_insert_sequential_threshold = ref 128
let treap_insert_height_threshold = ref 7
let treap_search_sequential_threshold = ref 100
let treap_search_height_threshold = ref 6
let treap_binary_search_threshold = ref 50

let treap_insert_type = ref 1
(*
  0: parallelise equal sub-batches, split tree accordingly
  1: always split at root node, binary search in insert array
  2: always split at root node, linear search in insert array
  3: always split at root node, binary & linear search in insert array
  *)

let treap_search_type = ref 2
(*
  0: parallelise all queries, start at root node
  1: parallelise equal sub-batches, start at root node
  2: always split at root node, binary search in search array
  3: always split at root node, linear search in search array 
  4: always split at root node, binary & linear search in search array
 *)

module Make (V: Map.OrderedType) = struct
  module Sequential = struct
    type 'a node = Leaf | Node of {
      key: V.t;
      mutable nval: 'a;
      mutable priority: int;
      mutable parent: 'a node;
      mutable left: 'a node;
      mutable right: 'a node
    }

    type 'a tree = {
      mutable root: 'a node
    }

    type side = Left | Right

    let key n =
      match n with
      | Leaf -> failwith "Key function: n is a leaf"
      | Node n' -> n'.key

    let priority n = 
      match n with
      | Leaf -> 0
      | Node n' -> n'.priority

    let left n =
      match n with
      | Leaf -> failwith "Left function: n is a leaf"
      | Node n' -> n'.left

    let right n =
      match n with
      | Leaf -> failwith "Right function: n is a leaf"
      | Node n' -> n'.right

    let parent n =
      match n with
      | Leaf -> failwith "Parent function: n is a leaf"
      | Node n' -> n'.parent

    let nval n =
      match n with
      | Leaf -> failwith "Value function: n is a leaf"
      | Node n' -> n'.nval

    let set_parent n p =
      match n with
      | Leaf -> ()
      | Node n' -> n'.parent <- p

    let set_child n s c =
      match n with
      | Leaf -> ()
      | Node n' ->
        match s with
        | Left -> (set_parent c n; n'.left <- c)
        | Right -> (set_parent c n; n'.right <- c)

    let expose n =
      match n with
      | Leaf -> failwith "Expose function: n is a leaf"
      | Node n' ->
        set_parent n'.left Leaf;
        set_parent n'.right Leaf;
        let l = n'.left in
        let r = n'.right in
        set_child n Left Leaf;
        set_child n Right Leaf;
        (l, n, r)

    let merge_three_nodes nl n nr =
      match n with
      | Leaf -> failwith "Merge three nodes function: n is a leaf"
      | Node _ ->
        set_child n Left nl;
        set_child n Right nr

    let root_node t = t.root

    let num_nodes t =
      let rec aux n =
        match n with
        | Leaf -> 0
        | Node n' -> 1 + aux n'.left + aux n'.right in
      aux t.root

    let flatten t =
      let rec flatten_aux n =
        match n with
        | Leaf -> []
        | Node n' -> (flatten_aux n'.left) @ [(n'.key, n'.nval)] @ (flatten_aux n'.right) in
      flatten_aux t.root

    let new_node k v =
      Node {
        key = k;
        nval = v;
        priority = Random.int @@ (Int.shift_left 1 30) - 1;
        parent = Leaf;
        left = Leaf;
        right = Leaf
      }

    let new_tree () = {root = Leaf}

    let new_tree_with_node n = {root = n}

    let rec search_aux k n =
      match n with
      | Leaf -> None
      | Node n' ->
        if k == n'.key then Some n'.nval
        else if k > n'.key then search_aux k n'.right
        else search_aux k n'.left

    let search k t = search_aux k t.root

    let rotate_left x t =
      let y = right x in
      set_child x Right (left y);
      if left y != Leaf then set_parent (left y) x;
      set_parent y (parent x);
      if parent x = Leaf then t.root <- y
      else if x == left @@ parent x then set_child (parent x) Left y
      else set_child (parent x) Right y;
      set_child y Left x

    let rotate_right x t =
      let y = left x in
      set_child x Left (right y);
      if right y != Leaf then set_parent (right y) x;
      set_parent y (parent x);
      if parent x = Leaf then t.root <- y
      else if x == right @@ parent x then set_child (parent x) Right y
      else set_child (parent x) Left y;
      set_child y Right x

    let rec rebalance n t =
      if n = Leaf then () else if parent n = Leaf then () else
      if priority n <= priority @@ parent n then () else
      if priority n > priority @@ parent n then begin
        if n == left @@ parent n then rotate_right (parent n) t
        else rotate_left (parent n) t;
        rebalance n t  (* n is the "new" parent *)
      end
    
    let rec insert_aux new_node current_node t =
      if key new_node == key current_node then ()
      else begin
        if key new_node < key current_node then begin
          if left current_node == Leaf then
            (set_child current_node Left new_node; rebalance new_node t)
          else insert_aux new_node (left current_node) t
        end
        else
          if right current_node == Leaf then
            (set_child current_node Right new_node; rebalance new_node t)
          else insert_aux new_node (right current_node) t
      end

    let insert k v t =
      let new_node = new_node k v in
      if t.root == Leaf then t.root <- new_node
      else insert_aux new_node t.root t

    let rec join tl n tr =
      if priority n >= priority tl.root && priority n >= priority tr.root then
        (merge_three_nodes tl.root n tr.root; {root = n})
      else begin
        if priority tl.root >= priority tr.root then
          let l1, n1, r1 = expose tl.root in
          let nt = join {root = r1} n tr in
          (merge_three_nodes l1 n1 nt.root; {root = n1})
        else
          let l2, n2, r2 = expose tr.root in
          let nt = join tl n {root = l2} in
          (merge_three_nodes nt.root n2 r2; {root = n2})
      end

    let rec split t k =
      if t.root = Leaf then ({root = Leaf}, Leaf, {root = Leaf})
      else
        let (l, m, r) = expose t.root in
        if k = key m then ({root = l}, m, {root = r})
        else if k < key m then
          let (ll, b, lr) = split {root = l} k in
          (ll, b, join lr m {root = r})
        else
          let (rl, b, rr) = split {root = r} k in
          (join {root = l} m rl, b, rr)

    let rec verify_heap_property_aux n =
      match n with
      | Leaf -> true
      | Node n' -> if n'.priority > priority n'.left && n'.priority > priority n'.right then
          verify_heap_property_aux n'.left && verify_heap_property_aux n'.right
        else false 

    let verify_heap_property t = verify_heap_property_aux t.root
  end

  type 'a t = 'a Sequential.tree

  type ('a, 'b) op =
    | Insert : V.t * 'a -> ('a, unit) op
    | Search : V.t -> ('a, 'a option) op

  type 'a wrapped_op = Mk : ('a, 'b) op * ('b -> unit) -> 'a wrapped_op

  let init () = Sequential.new_tree ()

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

  (** Use both binary search and linear search to traverse operations array *)
  let rec par_search_aux_4 op_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if node = Sequential.Leaf then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ Sequential.search_aux k node done 
    else
      let k = Sequential.key node in
      let nval = Sequential.nval node in
      let s1 = ref rstart and s2 = ref rstart in
      if n > 200 then begin
        let split = binary_search keys k rstart rstop in
        s1 := split; s2 := split;
        while !s1 > rstart && fst keys.(!s1 - 1) = k do
          s1 := !s1 - 1;
          snd keys.(!s1) @@ Some nval; 
        done;
        while !s2 < rstop && fst keys.(!s2) = k do
          snd keys.(!s2) @@ Some nval;
          s2 := !s2 + 1
        done;
      end
      else begin
        while !s1 < rstop && fst keys.(!s1) < k do s1 := !s1 + 1 done;
        s2 := !s1;
        while !s2 < rstop && fst keys.(!s2) = k do
          snd keys.(!s2) (Some nval);
          s2 := !s2 + 1
        done;
      end;
      let l = Domainslib.Task.async pool 
        (fun () -> par_search_aux_4 op_threshold ~pool (Sequential.left node) ~keys ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_4 op_threshold ~pool (Sequential.right node) ~keys ~range:(!s2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r

  (* Split the search operations only *)
  let rec par_search_aux_1 threshold pool t ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n > threshold then
      let num_par = n / threshold + if n mod threshold > 0 then 1 else 0 in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(num_par - 1) ~body:(fun i ->
        par_search_aux_1 threshold pool t ~keys ~range:(rstart + i * threshold, min rstop @@ rstart + (i + 1) * threshold)
      );
    else
      for i = rstart to rstop - 1 do
        let (k, kont) = keys.(i) in kont @@ Sequential.search k t
      done

  (** Use binary search only to traverse operations array *)
  let rec par_search_aux_2 op_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if node = Sequential.Leaf then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ Sequential.search_aux k node done 
    else
      let k = Sequential.key node in
      let nval = Sequential.nval node in
      let split = binary_search keys k rstart rstop in
      let s1 = ref split and s2 = ref split in
      while !s1 > rstart && fst keys.(!s1 - 1) = k do
        s1 := !s1 - 1;
        snd keys.(!s1) @@ Some nval; 
      done;
      while !s2 < rstop && fst keys.(!s2) = k do
        snd keys.(!s2) @@ Some nval;
        s2 := !s2 + 1
      done;
      let l = Domainslib.Task.async pool 
        (fun () -> par_search_aux_2 op_threshold ~pool (Sequential.left node) ~keys ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_2 op_threshold ~pool (Sequential.right node) ~keys ~range:(!s2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r

  (** Use linear search only to traverse operations array *)
  let rec par_search_aux_3 op_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if node = Sequential.Leaf then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ Sequential.search_aux k node done 
    else
      let k = Sequential.key node in
      let nval = Sequential.nval node in
      let s1 = ref rstart and s2 = ref rstart in
      while !s1 < rstop && fst keys.(!s1) < k do s1 := !s1 + 1 done;
      s2 := !s1;
      while !s2 < rstop && fst keys.(!s2) = k do
        snd keys.(!s2) (Some nval);
        s2 := !s2 + 1
      done;
      let l = Domainslib.Task.async pool 
        (fun () -> par_search_aux_3 op_threshold ~pool (Sequential.left node) ~keys ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_3 op_threshold ~pool (Sequential.right node) ~keys ~range:(!s2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r

  let par_search ?search_threshold ~pool (t: 'a t) keys =
    let search_threshold = match search_threshold with Some t -> t | None -> !treap_search_sequential_threshold in
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> V.compare k k') keys;
    match !treap_search_type with
    | 0 -> Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length keys - 1) ~body:(fun i ->
      let (k,kont) = keys.(i) in
      kont @@ Sequential.search k t)
    | 1 -> par_search_aux_1 search_threshold pool t ~keys ~range:(0, Array.length keys)
    | 2 -> par_search_aux_2 search_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)
    | 3 -> par_search_aux_3 search_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)
    | 4 -> par_search_aux_4 search_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)
    | _ -> failwith "Invalid search type"

  (** Mix of binary and linear search in partitioning insert array *)
  let rec par_insert_aux_3 op_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do
        let (k, v) = inserts.(i) in
        Sequential.insert k v t
      done
    else
      let (ln, mn, rn) = Sequential.expose @@ Sequential.root_node t in
      let lt = Sequential.new_tree_with_node ln and rt = Sequential.new_tree_with_node rn in
      let s1 = ref rstart and s2 = ref rstart in
      if n > !treap_binary_search_threshold then begin
        let k = Sequential.key mn in
        let split = binary_search inserts k rstart rstop in
        s1 := split; s2 := split;
        while !s1 > rstart && fst inserts.(!s1 - 1) = k do s1 := !s1 - 1 done;
        while fst inserts.(!s2) = k && !s2 >= rstop do s2 := !s2 + 1 done;
      end
      else begin
        while !s1 < rstop && fst inserts.(!s1) < Sequential.key mn do s1 := !s1 + 1 done;
        s2 := !s1;
        while !s2 < rstop && fst inserts.(!s2) = Sequential.key mn do s2 := !s2 + 1 done;
      end;
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux_3 op_threshold ~pool lt ~inserts ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_3 op_threshold ~pool rt ~inserts ~range:(!s2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      let nt = Sequential.join lt mn rt in
      t.root <- nt.root

  (** Linear traversal of inserts *)
  let rec par_insert_aux_2 op_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do
        let (k, v) = inserts.(i) in
        Sequential.insert k v t
      done
    else
      let (ln, mn, rn) = Sequential.expose @@ Sequential.root_node t in
      let lt = {Sequential.root = ln} and rt = {Sequential.root = rn} in
      let mid1 = ref rstart in
      while !mid1 < rstop && fst inserts.(!mid1) < Sequential.key mn do mid1 := !mid1 + 1 done;
      let mid2 = ref !mid1 in
      while !mid2 < rstop && fst inserts.(!mid2) <= Sequential.key mn do mid2 := !mid2 + 1 done;
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux_2 op_threshold ~pool lt ~inserts ~range:(rstart, !mid1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_2 op_threshold ~pool rt ~inserts ~range:(!mid2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      let nt = Sequential.join lt mn rt in
      t.root <- nt.root

  (* Use binary search on the sorted inserts array *)
  let rec par_insert_aux_1 op_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do
        let (k, v) = inserts.(i) in
        Sequential.insert k v t
      done
    else
      let (ln, mn, rn) = Sequential.expose @@ Sequential.root_node t in
      let lt = Sequential.new_tree_with_node ln and rt = Sequential.new_tree_with_node rn in
      let k = Sequential.key mn in
      let split = binary_search inserts k rstart rstop in
      let s1 = ref split and s2 = ref split in
      while !s1 > rstart && fst inserts.(!s1 - 1) = k do s1 := !s1 - 1 done;
      while fst inserts.(!s2) = k && !s2 >= rstop do s2 := !s2 + 1 done;
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux_1 op_threshold ~pool lt ~inserts ~range:(rstart, split)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_1 op_threshold ~pool rt ~inserts ~range:(split, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      let nt = Sequential.join lt mn rt in
      t.root <- nt.root

  (** Split cleanly down the middle of the insertion array *)
  let rec par_insert_aux_0 threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= threshold then
      for i = rstart to rstop - 1 do
        let (k, v) = inserts.(i) in
        Sequential.insert k v t
      done
    else
      let mid = rstart + n / 2 in
      let (mk, nv) = inserts.(mid) in
      let (lt, mn, rt) = Sequential.split t mk in
      let nn = match mn with
      | Leaf -> Sequential.new_node mk nv 
      | Node _ -> mn in
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux_0 threshold ~pool lt ~inserts ~range:(rstart, mid)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_0 threshold ~pool rt ~inserts ~range:(mid + 1, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      let (nlt, _, _) = Sequential.split lt (Sequential.key nn) in (* Make sure there's no duplicate *)
      let (_, _, nrt) = Sequential.split rt (Sequential.key nn) in
      let nt = Sequential.join lt nn rt in
      t.root <- nt.root

  let par_insert ?threshold ~pool (t: 'a t) inserts =
    let threshold = match threshold with Some t -> t | None -> !treap_insert_sequential_threshold in
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> V.compare k k') inserts;
    match !treap_insert_type with
    | 0 -> par_insert_aux_0 threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 1 -> par_insert_aux_1 threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 2 -> par_insert_aux_2 threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 3 -> par_insert_aux_3 threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | _ -> failwith "Invalid insert type"

  let run (type a) (t: a t) (pool: Domainslib.Task.pool) (ops: a wrapped_op array) =
    let searches: (V.t * (a option -> unit)) list ref = ref [] in
    let inserts: (V.t * a) list ref = ref [] in
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
      Sort.sort pool ~compare:(fun (k1,_) (k2,_) -> V.compare k1 k2) inserts;
      par_insert ~pool t inserts
    end

end