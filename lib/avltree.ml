module Sequential (V: Map.OrderedType) = struct
  type kt = V.t

  type side = Left | Right

  type 'a node = Leaf | Node of {
    mutable key: V.t;
    mutable nval: 'a;
    mutable height: int;
    mutable parent: 'a node; (* 'a node ref? *)
    mutable left: 'a node;
    mutable right: 'a node
  }

  (* type 'a tree = 'a node ref  *)
  type 'a t = {
    mutable root: 'a node (* root: 'a node ref *)
  }

  let key n =
    match n with
    | Leaf -> failwith "Key function: n is a leaf"
    | Node n' -> n'.key

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

  let get_balance n =
    match n with
    | Leaf -> 0
    | Node n' -> height n'.left - height n'.right

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

  let init () = {root = Leaf}

  let new_node k v = Node {
    key = k;
    nval = v;
    height = 1;
    parent = Leaf;
    left = Leaf;
    right = Leaf
  }

  let rec search_aux k n =
    match n with
    | Leaf -> None
    | Node n' ->
      if k = n'.key then Some n'.nval
      else if k > n'.key then search_aux k n'.right
      else search_aux k n'.left

  let search k t = search_aux k t.root

  let rotate_left x t =
    let y = right x in
    set_child x Right (left y);
    if left y <> Leaf then set_parent (left y) x;
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
    if right y <> Leaf then set_parent (right y) x;
    set_parent y (parent x);
    if parent x = Leaf then t.root <- y
    else if x == right @@ parent x then set_child (parent x) Right y
    else set_child (parent x) Left y;
    set_child y Right x;
    set_height x @@ 1 + max (height @@ left x) (height @@ right x);
    set_height y @@ 1 + max (height @@ left y) (height @@ right y)

  let rec rebalance_node n t =
    if n = Leaf then ()
    else begin
      set_height n @@ max (height (left n)) (height (right n)) + 1;
      let np = parent n in
      let balance = get_balance n in
      if balance > 1 then
        if height (left (left n)) >= height (right (left n)) then
          rotate_right n t
        else (rotate_left (left n) t; rotate_right n t)
      else if balance < -1 then
        if height (right (right n)) >= height (left (right n)) then
          rotate_left n t
        else (rotate_right (right n) t; rotate_left n t);
      rebalance_node np t
    end

  let rec insert_aux new_node current_node t =
    if key new_node = key current_node then ()
    else begin
      if key new_node < key current_node then
        if left current_node = Leaf then
          (set_child current_node Left new_node;
          rebalance_node current_node t)
        else insert_aux new_node (left current_node) t
      else
        if right current_node = Leaf then
          (set_child current_node Right new_node;
          rebalance_node current_node t)
        else insert_aux new_node (right current_node) t
    end

  let insert k v t =
    let new_node = new_node k v in
    if t.root = Leaf then t.root <- new_node
    else insert_aux new_node t.root t

  let rec find_min_node n =
    match n with
    | Leaf -> failwith "Find min node function: n is a leaf"
    | Node n' ->
      if n'.left == Leaf then n
      else find_min_node (n'.left)

  let rec delete_aux current_node k t =
    if current_node = Leaf then ()
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
          set_child p Left (right current_node);
        rebalance_node current_node t)
      else if right current_node = Leaf then
        (if p == Leaf then
          let (l, _, _) = expose current_node in
          t.root <- l
        else if right p == current_node then
          set_child p Right (left current_node)
        else
          set_child p Left (left current_node);
        rebalance_node current_node t)
      else
        let min_node = find_min_node (right current_node) in
        match current_node with
        | Leaf -> failwith "impossible error"
        | Node n' ->
          n'.key <- key min_node;
          n'.nval <- nval min_node;
          delete_aux n'.right (key min_node) t;
    end

  let delete k t =
    if t.root = Leaf then ()
    else delete_aux t.root k t

  let rec verify_height_invariant n =
    match n with
    | Leaf -> true
    | Node n' ->
      let height_diff = abs @@ height n'.left - height n'.right in
      height_diff <= 1 && 
      verify_height_invariant n'.left && verify_height_invariant n'.right
end

module Prebatch (V: Map.OrderedType) = struct

  module S = Sequential(V)
  let compare = V.compare
  let key = S.key
  let size_factor = S.height
  let search_node = S.search_aux

  let peek_root (t: 'a S.t) = t.root

  let peek_node (n: 'a S.node) =
    match n with
    | Leaf -> failwith "Peek node function: n is a leaf"
    | Node n' -> ([|n'.key|], [|n'.nval|], [|n'.left; n'.right|])

  let merge_three_nodes (nl: 'a S.node) (n: 'a S.node) (nr: 'a S.node) =
    match n with
    | Leaf -> failwith "Merge three nodes function: n is a leaf"
    | Node _ ->
      S.set_child n Left nl;
      S.set_child n Right nr;
      S.set_height n ((max (size_factor nl) (size_factor nr)) + 1)

  let rec join_right (tl: 'a S.t) k (tr: 'a S.t) =
    let (l, k', c) = S.expose tl.root in
    if size_factor c <= size_factor tr.root + 1 then begin
      merge_three_nodes c k tr.root;
      let t' = {S.root = k} in
      if size_factor t'.root <= size_factor l + 1 then
        (merge_three_nodes l k' t'.root; {S.root = k'})
      else begin
        S.rotate_right k t';
        merge_three_nodes l k' t'.root;
        let t'' = {S.root = k'} in
        S.rotate_left k' t''; t''
      end
    end
    else begin
      let t' = join_right {root = c} k tr in
      merge_three_nodes l k' t'.root;
      let t'' = {S.root = k'} in
      if size_factor t'.root <= size_factor l + 1 then t''
      else begin
        S.rotate_left k' t''; t''
      end
    end

  let rec join_left (tl: 'a S.t) n (tr: 'a S.t) =
    let (c, n', r) = S.expose tr.root in
    if size_factor c <= size_factor tl.root + 1 then begin
      merge_three_nodes tl.root n c;
      let t' = {S.root = n} in
      if size_factor n <= size_factor r + 1 then
        (merge_three_nodes n n' r; {S.root = n'})
      else begin
        S.rotate_left n t';
        merge_three_nodes t'.root n' r;
        let t'' = {S.root = n'} in
        S.rotate_right n' t''; t''
      end
    end
    else begin
      let t' = join_left tl n {root = c} in
      merge_three_nodes t'.root n' r;
      let t'' = {S.root = n'} in
      if size_factor t'.root <= size_factor r + 1 then t''
      else begin
        S.rotate_right n' t''; t''
      end
    end

  let join_with_node (tl: 'a S.t) (n: 'a S.node) (tr: 'a S.t) =
    if size_factor tl.root > size_factor tr.root + 1 then
      join_right tl n tr
    else if size_factor tr.root > size_factor tl.root + 1 then
      join_left tl n tr
    else begin
      merge_three_nodes tl.root n tr.root; {root = n}
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

  let size_factor (t: 'a S.t) = S.height t.root (* Redefine to fit Prebatch functor *)

end