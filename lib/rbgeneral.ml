module Sequential (V: Map.OrderedType) = struct
  type colour = Red | Black;;

  type side = Left | Right;;

  type kt = V.t;;

  type 'a node = Leaf | Node of {
    mutable key: V.t;
    mutable tval: 'a;
    mutable colour: colour;
    mutable rl: side;
    mutable bheight: int;
    mutable size: int;
    mutable parent: 'a node;
    mutable left: 'a node;
    mutable right: 'a node
  }

  type 'a t = {
    mutable root: 'a node
  }

  let compare = V.compare

  let peek n =
    match n with
    | Leaf -> failwith "Peek function: n is a leaf"
    | Node n -> (n.left, n.right)

  let colour n =
    match n with
    | Leaf -> Black
    | Node n' -> n'.colour

  let get_node_key n =
    match n with
    | Leaf -> failwith "Key function: n is a leaf"
    | Node n' -> n'.key

  let value n =
    match n with
    | Leaf -> failwith "Value function: n is a leaf"
    | Node n' -> n'.tval

  let left n =
    match n with
    | Leaf -> failwith "Left function: n is a leaf"
    | Node n' -> n'.left

  let right n =
    match n with
    | Leaf -> failwith "Right function: n is a leaf"
    | Node n' -> n'.right

  let key n =
    match n with
    | Leaf -> failwith "Key function: n is a leaf"
    | Node n' -> n'.key
  
  let bheight n =
    match n with
    | Leaf -> 1
    | Node n' -> n'.bheight

  let size_node n =
    match n with
    | Leaf -> 0
    | Node n' -> n'.size

  let size t = size_node t.root

  let set_bheight n h =
    match n with
    | Leaf -> ()
    | Node n' -> n'.bheight <- h

  let update_bheight n =
    match n with
    | Leaf -> ()
    | Node n' ->
      n'.bheight <- max (bheight @@ left n) (bheight @@ right n)
        + if colour n = Black then 1 else 0

  let update_size n =
    match n with
    | Leaf -> ()
    | Node n' ->
      n'.size <- size_node n'.right + size_node n'.left + 1

  let parent n =
    match n with
    | Leaf -> failwith "Parent function: n is a leaf"
    | Node n' -> n'.parent

  let set_parent n p =
    match n with
    | Leaf -> ()
    | Node n' -> n'.parent <- p

  let side n =
    match n with
    | Leaf -> failwith "Side function: n is a leaf"
    | Node n' -> n'.rl

  let set_side n rl =
    match n with
    | Leaf -> ()
    | Node n' -> n'.rl <- rl

  (** Takes in node, side, child. Updates child's parent field as well. *)
  let set_child n s c =
    match n with
    | Leaf -> ()
    | Node n' ->
      match s with
      | Left -> (set_parent c n; set_side c Left; n'.left <- c)
      | Right -> (set_parent c n; set_side c Right; n'.right <- c)

  let set_colour n c =
    match n with
    | Leaf -> ()
    | Node n' ->
      n'.colour <- c;
      update_bheight n

  (** Break apart a node, returns (left child, node, right child). The children's parents are set to Leaf, and the parent's children as well. *)
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
      update_bheight n;
      update_size n;
      (l, n, r)

  let merge_three_nodes nl n nr =
    match n with
    | Leaf -> failwith "Merge three nodes function: n is a leaf"
    | Node _ ->
      set_child n Left nl;
      set_child n Right nr;
      update_bheight n;
      update_size n

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
      | Node n' -> (flatten_aux n'.left) @ [(n'.key, n'.tval)] @ (flatten_aux n'.right) in
    flatten_aux t.root

  let init () = {root = Leaf}

  let new_tree_with_node n = {root = n}

  let new_node k v = Node {
    key = k;
    tval = v;
    colour = Red;
    rl = Left;
    bheight = 1;
    size = 1;
    parent = Leaf;
    left = Leaf;
    right = Leaf
  }

  let rec search_aux k n =
    match n with
    | Leaf -> None
    | Node n' ->
      if k == n'.key then Some n'.tval
      else if k > n'.key then search_aux k n'.right
      else search_aux k n'.left

  let search k t = search_aux k t.root

  let rec find_min_node n =
    match n with
    | Leaf -> None
    | Node n' ->
      if n'.left == Leaf then Some n
      else find_min_node (n'.left)

  let rec find_max_node n =
    match n with
    | Leaf -> None
    | Node n' ->
      if n'.right == Leaf then Some n
      else find_max_node (n'.right)

  let find_min_key t = Option.map key @@ find_min_node t.root

  let find_max_key t = Option.map key @@ find_max_node t.root

  let find_min_node n = 
    match find_min_node n with
    | None -> failwith "Find min node function: n is a leaf"
    | Some n' -> n'

  let find_max_node n =
    match find_max_node n with
    | None -> failwith "Find min node function: n is a leaf"
    | Some n' -> n'

  let predecessor k t =
    let rec aux n cur_pred = 
      match n with
      | Leaf -> cur_pred
      | Node n' ->
        if n'.key == k then
          if n'.left != Leaf then
            Some (key (find_max_node n'.left))
          else cur_pred
        else if n'.key < k then
          aux n'.right (Some n'.key)
        else aux n'.left cur_pred in
    aux t.root None

  let successor k t =
    let rec aux n cur_succ = 
      match n with
      | Leaf -> cur_succ
      | Node n' ->
        if n'.key == k then
          if n'.right != Leaf then
            Some (key (find_min_node n'.right))
          else cur_succ
        else if n'.key > k then
          aux n'.left (Some n'.key)
        else aux n'.right cur_succ in
    aux t.root None

  let rotate_left x t =
    let y = right x in
    set_child x Right (left y);
    if left y != Leaf then set_parent (left y) x;
    set_parent y (parent x);
    if parent x = Leaf then t.root <- y
    else if x == left @@ parent x then set_child (parent x) Left y
    else set_child (parent x) Right y;
    set_child y Left x;
    update_bheight x; update_bheight y;
    update_size x; update_size y

  let rotate_right x t =
    let y = left x in
    set_child x Left (right y);
    if right y != Leaf then set_parent (right y) x;
    set_parent y (parent x);
    if parent x = Leaf then t.root <- y
    else if x == right @@ parent x then set_child (parent x) Right y
    else set_child (parent x) Left y;
    set_child y Right x;
    update_bheight x; update_bheight y;
    update_size x; update_size y

  let rec update_size_rec n =
    match n with
    | Leaf -> failwith "Cannot call update_size_rec on Leaf node"
    | Node n' ->
      update_size n; if n'.parent != Leaf then update_size_rec n'.parent

  let rec fix_insert n t =
    if colour (parent n) = Black then set_colour t.root Black
    else if parent n = Leaf || parent (parent n) = Leaf then set_colour t.root Black
    else begin
      let n_parent = parent n in let n_grandparent = parent n_parent in
      let (parent_side, uncle) =
        if n_parent == right n_grandparent then (Right, left n_grandparent)
        else (Left, right n_grandparent) in
      update_bheight n_parent;
      update_bheight n_grandparent;
      if colour uncle == Red then
        (set_colour uncle Black;
        set_colour n_parent Black;
        set_colour n_grandparent Red;
        fix_insert n_grandparent t)
      else begin
        let n' = ref n in
        if parent_side = Right && n == left n_parent then
          (n' := n_parent; rotate_right !n' t)
        else if parent_side = Left && n == right n_parent then
          (n' := n_parent; rotate_left !n' t);
        set_colour (parent !n') Black;
        set_colour (parent @@ parent !n') Red;
        if parent_side = Right then
          rotate_left (parent @@ parent !n') t
        else rotate_right (parent @@ parent !n') t
      end
    end

  let rec insert_aux new_node current_node t =
    let k = get_node_key new_node in
    match current_node with
    | Leaf -> failwith "This is not supposed to happen"
    | Node n' ->
      if n'.key = k then ()
      else if n'.key > k then
        (if n'.left = Leaf then
          (set_child current_node Left new_node;
          update_size_rec current_node;
          fix_insert new_node t)
        else insert_aux new_node n'.left t)
      else if n'.right = Leaf then
        (set_child current_node Right new_node;
        update_size_rec current_node;
        fix_insert new_node t)
      else insert_aux new_node n'.right t

  let insert k v t =
    let new_node = new_node k v in
    match new_node with
    | Leaf -> failwith "Can't insert Leaf"
    | Node n ->
      match t.root with
      | Leaf -> (n.colour <- Black; n.bheight <- 2; n.parent <- Leaf; t.root <- new_node)
      | Node _ -> (n.colour <- Red; insert_aux new_node t.root t)

  

  let rec update_bheight_rec n =
    update_bheight n;
    let p = parent n in
    if p != Leaf then update_bheight_rec p

  let rec fix_delete n p t =
    if n == t.root || colour n == Red then
      (set_colour n Black; update_bheight_rec n; update_size n)
    else begin
      update_bheight n;
      update_bheight p;
      update_size n;
      update_size p;
      if n == left p then begin
        let w = ref (right p) in
        if colour !w == Red then begin
          set_colour !w Black;
          set_colour p Red;
          rotate_left p t;
          w := right p;
        end;
        if colour (left !w) == Black && colour (right !w) == Black then begin
          set_colour !w Red;
          if parent p != Leaf then fix_delete p (parent p) t
          else set_colour p Black
        end
        else begin
          if colour (right !w) == Black then begin
            set_colour (left !w) Black;
            set_colour !w Red;
            rotate_right !w t;
            w := right p;
          end;
          set_colour !w (colour p);
          set_colour p Black;
          set_colour (right !w) Black;
          rotate_left p t;
          set_colour t.root Black;
          update_bheight_rec p
        end
      end
      else begin
        let w = ref (left p) in
        if colour !w == Red then begin
          set_colour !w Black;
          set_colour p Red;
          rotate_right p t;
          w := left p;
        end;
        if colour (left !w) == Black && colour (right !w) == Black then begin
          set_colour !w Red;
          if parent p != Leaf then fix_delete p (parent p) t
          else set_colour p Black
        end
        else begin
          if colour (left !w) == Black then begin
            set_colour (right !w) Black;
            set_colour !w Red;
            rotate_left !w t;
            w := left p;
          end;
          set_colour !w (colour p);
          set_colour p Black;
          set_colour (left !w) Black;
          rotate_right p t;
          set_colour t.root Black;
          update_bheight_rec p
        end
      end
    end

  let rec delete_aux current_node k t =
    if current_node == Leaf then ()
    else if k < key current_node then
      delete_aux (left current_node) k t
    else if key current_node < k then
      delete_aux (right current_node) k t
    else begin
      let p = parent current_node in
      if left current_node = Leaf then
        if p == Leaf then
          let (_, _, r) = expose current_node in
          (t.root <- r; set_colour t.root Black)
        else begin
          let r = right current_node in
          if right p == current_node
          then set_child p Right r
          else set_child p Left r;
          update_size_rec p;
          if colour current_node = Black then fix_delete r p t
          else update_bheight_rec p
        end
      else if right current_node = Leaf then
        if p == Leaf then
          let (l, _, _) = expose current_node in
          (t.root <- l; set_colour t.root Black)
        else begin
          let l = left current_node in
          if right p == current_node
          then set_child p Right l
          else set_child p Left l;
          update_size_rec p;
          if colour current_node = Black then fix_delete l p t
          else update_bheight_rec p
        end
      else
        let min_node = find_min_node (right current_node) in
        match current_node with
        | Leaf -> failwith "impossible error"
        | Node n' ->
          n'.key <- key min_node;
          n'.tval <- value min_node;
          delete_aux n'.right (key min_node) t
    end

  let delete k t =
    if t.root = Leaf then ()
    else delete_aux t.root k t

  (* Inefficient delete function, based on splitting and rejoining the trees *)
  (* let delete k t =
    let (l, _, r) = split t k in
    let rmin = find_min_node r.root in
    let (_, mn, r') = split r (key rmin) in
    t.root <- (join l mn r').root *)

  let rec verify_black_depth n = 
    match n with
    | Leaf -> (true, 1) (* Leaves are always Black *)
    | Node n' ->
      let cur_depth = if n'.colour = Black then 1 else 0 in
      let meta_depth = n'.bheight in
      let (lb, ld) = verify_black_depth n'.left in
      let (rb, rd) = verify_black_depth n'.right in
      (* Printf.printf "key:%d, color:%s, metadata:%d, realleft:%d, realright:%d\n" n'.key (if n'.colour == Black then "Black" else "Red") meta_depth ld rd; *)
      if lb && rb && ld = rd
        && ld + cur_depth = meta_depth
      then (true, ld + cur_depth) else (false, ld + cur_depth)

  let rec verify_internal_property n =
    match n with
    | Leaf -> true
    | Node n' ->
      if n'.colour = Red then
        if colour n'.left = Black && colour n'.right = Black then
          verify_internal_property n'.left && verify_internal_property n'.right
        else false
      else verify_internal_property n'.left && verify_internal_property n'.right

  let rec verify_parent_metadata n =
    match n with
    | Leaf -> true
    | Node n' ->
      let rb = match n'.right with
      | Leaf -> true
      | Node nrn' -> nrn'.parent = n && nrn'.rl = Right && verify_parent_metadata n'.right in
      if not rb then false else match n'.left with
      | Leaf -> true
      | Node nln' -> nln'.parent = n && nln'.rl = Left && verify_parent_metadata n'.left

  let verify_tree ?(check_root_color=false) t : bool =
    if check_root_color then colour t.root = Black
    else
      let (bd, _) = verify_black_depth t.root in
      bd && verify_internal_property t.root
end

module Prebatch (V: Map.OrderedType) = struct
  module S = Sequential(V)
  let compare = V.compare
  let key = S.key
  let size_factor = S.bheight
  let search_node = S.search_aux

  let peek_root (t: 'a S.t) = t.root

  let peek_node (n: 'a S.node) =
    match n with
    | Leaf -> failwith "Peek node function: n is a leaf"
    | Node n' -> ([|n'.key|], [|n'.tval|], [|n'.left; n'.right|])

  let get_rank n = 
    let bh = S.bheight n in
    if S.colour n = Black then 2 * (bh - 1) else 2 * bh - 1

  let rec join_right (tl: 'a S.t) n (tr: 'a S.t) =
    if get_rank tl.root = int_of_float (floor @@ float_of_int (get_rank tr.root) /. 2.) * 2 then
      (S.set_colour n Red; S.merge_three_nodes tl.root n tr.root; {S.root = n})
    else begin
      let (l, mn, r) = S.expose tl.root in
      let c = S.colour mn in
      let ntr = join_right {S.root = r} n tr in
      S.merge_three_nodes l mn ntr.root;
      let t'' = {S.root = mn} in
      if c == Black && S.colour (S.right mn) = Red && S.colour (S.right @@ S.right mn) = Red then begin
        S.set_colour (S.right @@ S.right mn) Black;
        S.rotate_left mn t''
      end; t''
    end
    ;;

  let rec join_left (tl: 'a S.t) n (tr: 'a S.t) =
    if get_rank tr.root = int_of_float (floor @@ float_of_int (get_rank tl.root) /. 2.) * 2 then
      (S.set_colour n Red; S.merge_three_nodes tl.root n tr.root; {S.root = n})
    else begin
      let (l, mn, r) = S.expose tr.root in
      let c = S.colour mn in
      let ntl = join_left tl n {root = l} in
      S.merge_three_nodes ntl.root mn r;
      let t'' = {S.root = mn} in
      if c == Black && S.colour (S.left mn) = Red && S.colour (S.left @@ S.left mn) = Red then begin
        S.set_colour (S.left @@ S.left mn) Black;
        S.rotate_right mn t''
      end; t''
    end

  let join_with_node (tl: 'a S.t) n (tr: 'a S.t) =
    let ctl = int_of_float (floor @@ float_of_int (get_rank tl.root) /. 2.) in
    let ctr = int_of_float (floor @@ float_of_int (get_rank tr.root) /. 2.) in
    if ctl > ctr then
      let nt = join_right tl n tr in
      if S.colour nt.root = Red && S.colour (S.right nt.root) = Red then
        S.set_colour nt.root Black;
      nt
    else if ctr > ctl then
      let nt = join_left tl n tr in
      if S.colour nt.root = Red && S.colour (S.left nt.root) = Red then
        S.set_colour nt.root Black;
      nt
    else if S.colour tl.root = Black && S.colour tr.root = Black then
      (S.set_colour n Red; S.merge_three_nodes tl.root n tr.root; {root = n})
    else (S.set_colour n Black; S.merge_three_nodes tl.root n tr.root; {root = n})

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

  let join_two (tl: 'a S.t) (tr: 'a S.t) =
    if tr.root = Leaf then tl
    else
      let (_, mn, r) = split_with_node tr (S.find_min_node tr.root |> key) in
      join_with_node tl mn r

  let split_two (t: 'a S.t) k =
    let (l, m, r) = split_with_node t k in
    (match m with Node m' -> S.insert m'.key m'.tval r | _ -> ());
    (l, r)

  let join (t_arr: 'a S.t array) =
    let acc = ref t_arr.(0) in
    for i = 1 to Array.length t_arr - 1 do
      acc := join_two !acc t_arr.(i);
    done; !acc

  let split k_arr t =
    let acc = ref t and t_list = ref [] in
    for i = Array.length k_arr - 1 downto 0 do
      let (lt, rt) = split_two !acc k_arr.(i) in
      acc := lt; t_list := rt :: !t_list;
    done; Array.of_list @@ !acc :: !t_list

  let set_root (n: 'a S.node) (t: 'a S.t) = t.root <- n

  let break_node n =
    let (l, m, r) = S.expose n in
    ([|S.key m, S.value m|], [|{S.root = l}; {S.root = r}|])
end