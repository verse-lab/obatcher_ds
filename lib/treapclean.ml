module Sequential (V: Map.OrderedType) = struct
  type kt = V.t

  type side = Left | Right

  type 'a node = Leaf | Node of {
    mutable key: V.t;
    mutable nval: 'a;
    mutable height: int;
    mutable priority: int;
    mutable parent: 'a node;
    mutable left: 'a node;
    mutable right: 'a node
  }

  type 'a t = {
    mutable root: 'a node
  }

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

  let height n =
    match n with
    | Leaf -> 0
    | Node n' -> n'.height

  let parent n =
    match n with
    | Leaf -> failwith "Parent function: n is a leaf"
    | Node n' -> n'.parent

  let nval n =
    match n with
    | Leaf -> failwith "Value function: n is a leaf"
    | Node n' -> n'.nval

  let set_height n h =
    match n with
    | Leaf -> ()
    | Node n' -> n'.height <- h

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
      let l = n'.left and r = n'.right in
      set_parent l Leaf;
      set_parent r Leaf;
      set_child n Left Leaf;
      set_child n Right Leaf;
      set_height n 1;
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
      height = 1;
      priority = Random.int @@ (Int.shift_left 1 30) - 1;
      parent = Leaf;
      left = Leaf;
      right = Leaf
    }

  let init () = {root = Leaf}

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
    set_child y Left x;
    set_height x @@ 1 + max (height @@ left x) (height @@ right x);
    set_height y @@ 1 + max (height @@ left y) (height @@ right y)

  let rotate_right x t =
    let y = left x in
    set_child x Left (right y);
    if right y != Leaf then set_parent (right y) x;
    set_parent y (parent x);
    if parent x = Leaf then t.root <- y
    else if x == right @@ parent x then set_child (parent x) Right y
    else set_child (parent x) Left y;
    set_child y Right x;
    set_height x @@ 1 + max (height @@ left x) (height @@ right x);
    set_height y @@ 1 + max (height @@ left y) (height @@ right y)

  let rec rebalance n t =
    set_height n @@ max (height (left n)) (height (right n)) + 1;
    if n = Leaf then () else if parent n = Leaf then ()
    else if priority n <= priority @@ parent n then ()
    else if priority n > priority @@ parent n then begin
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

  let rec find_min_node n =
    match n with
    | Leaf -> failwith "Find min node function: n is a leaf"
    | Node n' ->
      if n'.left == Leaf then n
      else find_min_node (n'.left)

  let rec delete_aux current_node k t =
    if current_node == Leaf then ()
    else if k < key current_node then
      delete_aux (left current_node) k t
    else if key current_node < k then
      delete_aux (right current_node) k t
    else begin
      let p = parent current_node in
      if left current_node = Leaf then
        (if p == Leaf then
          let (_, _, r) = expose current_node in
          t.root <- r
        else if right p == current_node then
          set_child p Right (right current_node)
        else
          set_child p Left (right current_node))
      else if right current_node = Leaf then
        (if p == Leaf then
          let (l, _, _) = expose current_node in
          t.root <- l
        else if right p == current_node then
          set_child p Right (left current_node)
        else
          set_child p Left (left current_node))
      else
        let min_node = find_min_node (right current_node) in
        match current_node with
        | Leaf -> failwith "impossible error"
        | Node n' ->
          n'.key <- key min_node;
          n'.nval <- nval min_node;
          delete_aux n'.right (key min_node) t
    end

  let delete k t =
    if t.root == Leaf then ()
    else delete_aux t.root k t

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

module Prebatch (V: Map.OrderedType) = struct
  module S = Sequential(V)
  let compare = V.compare
  let key = S.key
  let size_factor (t: 'a S.t) = S.height t.root
  let search_node = S.search_aux

  let peek_root (t: 'a S.t) = t.root

  let peek_node (n: 'a S.node) =
    match n with
    | Leaf -> failwith "Peek node function: n is a leaf"
    | Node n' -> ([|n'.key|], [|n'.nval|], [|n'.left; n'.right|])

  let rec join_with_node (tl: 'a S.t) n (tr: 'a S.t) =
    if S.priority n >= S.priority tl.root && S.priority n >= S.priority tr.root then
      (S.merge_three_nodes tl.root n tr.root; {S.root = n})
    else begin
      if S.priority tl.root >= S.priority tr.root then
        let l1, n1, r1 = S.expose tl.root in
        let nt = join_with_node {root = r1} n tr in
        (S.merge_three_nodes l1 n1 nt.root; {S.root = n1})
      else
        let l2, n2, r2 = S.expose tr.root in
        let nt = join_with_node tl n {S.root = l2} in
        (S.merge_three_nodes nt.root n2 r2; {S.root = n2})
    end

  let rec split_with_node (t: 'a S.t) k =
    if t.root = Leaf then ({S.root = Leaf}, S.Leaf, {S.root = Leaf})
    else
      let (l, m, r) = S.expose t.root in
      if k = key m then ({root = l}, m, {root = r})
      else if k < key m then
        let (ll, b, lr) = split_with_node {root = l} k in
        (ll, b, join_with_node lr m {root = r})
      else
        let (rl, b, rr) = split_with_node {root = r} k in
        (join_with_node {root = l} m rl, b, rr)

  let join (tl: 'a S.t) (tr: 'a S.t) =
    if tr.root = Leaf then tl
    else
      let (_, mn, r) = split_with_node tr (S.find_min_node tr.root |> key) in
      join_with_node tl mn r

  let split (t: 'a S.t) k =
    let (l, m, r) = split_with_node t k in
    (match m with Node m' -> S.insert m'.key m'.nval r | _ -> ());
    (l, r)

  let set_root (t1: 'a S.t) (t2: 'a S.t) = t1.root <- t2.root

end