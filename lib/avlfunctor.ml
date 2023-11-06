[@@@warning "-32-26"]

module AvlTree (V: Map.OrderedType) = struct

  type kt = V.t

  type side = Left | Right

  type 'a node = Leaf | Node of {
    mutable key: kt;
    mutable nval: 'a;
    mutable height: int;
    mutable parent: 'a node;
    mutable left: 'a node;
    mutable right: 'a node
  }

  type 'a t = {
    mutable root: 'a node
  }

  let insert_op_threshold = ref 1000
  let search_op_threshold = ref 50
  let height_threshold = ref 5
  let search_type = ref 1
  let insert_type = ref 1

  let compare = V.compare

  let is_leaf n =
    match n with
    | Leaf -> true
    | Node _ -> false

  let is_empty t = t.root = Leaf

  let get_height t =
    match t.root with
    | Leaf -> 0
    | Node n -> n.height

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

  let value n =
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
      set_parent n'.left Leaf;
      set_parent n'.right Leaf;
      let l = n'.left in
      let r = n'.right in
      set_child n Left Leaf;
      set_child n Right Leaf;
      set_height n 1;
      (l, n, r)
      
  let merge_three_nodes nl n nr =
    match n with
    | Leaf -> failwith "Merge three nodes function: n is a leaf"
    | Node _ ->
      set_child n Left nl;
      set_child n Right nr;
      set_height n ((max (height nl) (height nr)) + 1)

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

  let new_tree () = {root = Leaf}

  let wrap_node n = {root = n}

  let unwrap_tree t = t.root

  let set_root_node n t = t.root <- n

  let peek n =
    match n with
    | Leaf -> failwith "Peek function: n is a leaf"
    | Node n -> (n.left, n.right)

  let new_node k v = Node {
    key = k;
    nval = v;
    (* rl = Left; *)
    height = 1;
    parent = Leaf;
    left = Leaf;
    right = Leaf
  }

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

  let rebalance_node n t =
    let () = set_height n @@ max (height (left n)) (height (right n)) + 1 in
    let balance = get_balance n in
    if balance > 1 then
      if height (left (left n)) > height (right (left n)) then
        rotate_right n t
      else (rotate_left (left n) t; rotate_right n t)
    else if balance < -1 then
      if height (right (right n)) > height (left (right n)) then
        rotate_left n t
      else (rotate_right (right n) t; rotate_left n t)

  let rec insert_aux new_node current_node t =
    if key new_node == key current_node then ()
    else begin
      let () = if key new_node < key current_node then
        if left current_node == Leaf then
          set_child current_node Left new_node
        else insert_aux new_node (left current_node) t
      else
        if right current_node == Leaf then
          set_child current_node Right new_node
        else insert_aux new_node (right current_node) t in
      rebalance_node current_node t
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
      (delete_aux (left current_node) k t; rebalance_node current_node t)
    else if key current_node < k then
      (delete_aux (right current_node) k t; rebalance_node current_node t)
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
          n'.nval <- value min_node;
          delete_aux n'.right (key min_node) t;
          rebalance_node current_node t
    end

  let delete k t =
    if t.root == Leaf then ()
    else delete_aux t.root k t

  let rec join_right tl k tr =
    let (l, k', c) = expose tl.root in
    if height c <= height tr.root + 1 then begin
      merge_three_nodes c k tr.root;
      let t' = {root = k} in
      if height t'.root <= height l + 1 then
        (merge_three_nodes l k' t'.root; {root = k'})
      else begin
        rotate_right k t';
        merge_three_nodes l k' t'.root;
        let t'' = {root = k'} in
        rotate_left k' t''; t''
      end
    end
    else begin
      let t' = join_right {root = c} k tr in
      merge_three_nodes l k' t'.root;
      let t'' = {root = k'} in
      if height t'.root <= height l + 1 then t''
      else begin
        rotate_left k' t''; t''
      end
    end

  let rec join_left tl n tr =
    let (c, n', r) = expose tr.root in
    if height c <= height tl.root + 1 then begin
      merge_three_nodes tl.root n c;
      let t' = {root = n} in
      if height n <= height r + 1 then
        (merge_three_nodes n n' r; {root = n'})
      else begin
        rotate_left n t';
        merge_three_nodes t'.root n' r;
        let t'' = {root = n'} in
        rotate_right n' t''; t''
      end
    end
    else begin
      let t' = join_left tl n {root = c} in
      merge_three_nodes t'.root n' r;
      let t'' = {root = n'} in
      if height t'.root <= height r + 1 then t''
      else begin
        rotate_right n' t''; t''
      end
    end

  let join tl n tr =
    if height tl.root > height tr.root + 1 then
      join_right tl n tr
    else if height tr.root > height tl.root + 1 then
      join_left tl n tr
    else begin
      merge_three_nodes tl.root n tr.root; {root = n}
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

  let rec verify_height_invariant n =
    match n with
    | Leaf -> true
    | Node n' ->
      let height_diff = abs @@ height n'.left - height n'.right in
      height_diff <= 1 && verify_height_invariant n'.left && verify_height_invariant n'.right

end