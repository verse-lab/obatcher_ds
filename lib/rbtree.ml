[@@@warning "-32-26"]
let rbtree_insert_sequential_threshold = ref 64
let rbtree_insert_height_threshold = ref 3
let rbtree_search_sequential_threshold = ref 100
let rbtree_search_height_threshold = ref 6
let rbtree_binary_search_threshold = ref 50
let rbtree_insert_type = ref 1
(*
  0: parallelise equal sub-batches, split tree accordingly
  1: always split at root node, binary search in insert array
  2: always split at root node, linear search in insert array
  3: always split at root node, binary & linear search in insert array
  *)

let rbtree_search_type = ref 2
(*
  0: parallelise all queries, start at root node
  1: parallelise equal sub-batches, start at root node
  2: always split at root node, binary search in search array
  3: always split at root node, linear search in search array
  4: always split at root node, binary & linear search in search array
 *)

module Make (V: Map.OrderedType) = struct

  module Sequential = struct
    type colour = Red | Black;;

    type side = Left | Right;;

    type 'a rb_node = Leaf | Node of {
      mutable key: V.t;
      mutable tval: 'a;
      mutable colour: colour;
      mutable rl: side;
      mutable bheight: int;
      mutable parent: 'a rb_node;
      mutable left: 'a rb_node;
      mutable right: 'a rb_node
    }

    type 'a rb_tree = {
      mutable root: 'a rb_node
    }

    let get_node_colour n =
      match n with
      | Leaf -> Black
      | Node n' -> n'.colour

    let get_node_key n =
      match n with
      | Leaf -> failwith "Key function: n is a leaf"
      | Node n' -> n'.key

    let tval n =
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

    let set_bheight n h =
      match n with
      | Leaf -> ()
      | Node n' -> n'.bheight <- h

    let update_bheight n =
      match n with
      | Leaf -> ()
      | Node n' ->
        n'.bheight <- max (bheight @@ left n) (bheight @@ right n)
          + if get_node_colour n = Black then 1 else 0

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
      | Node n' -> n'.colour <- c

    let set_colour_and_update_bheight n c =
      match n with
      | Leaf -> ()
      | Node n' ->
        if n'.colour = Black && c = Red then
          n'.bheight <- n'.bheight - 1
        else if n'.colour = Red && c = Black then
          n'.bheight <- n'.bheight + 1;
        n'.colour <- c

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
        set_bheight n @@ 1 + (if get_node_colour n = Black then 1 else 0);
        (l, n, r)

    let merge_three_nodes nl n nr =
      match n with
      | Leaf -> failwith "Merge three nodes function: n is a leaf"
      | Node _ ->
        set_child n Left nl;
        set_child n Right nr;
        set_bheight n (max (bheight nl) (bheight nr) + if get_node_colour n = Black then 1 else 0)

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

    let new_tree () = {root = Leaf}

    let new_tree_with_node n = {root = n}

    let new_node k v = Node {
      key = k;
      tval = v;
      colour = Red;
      rl = Left;
      bheight = 1;
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

    let rotate_left x t =
      let y = right x in
      set_child x Right (left y);
      if left y != Leaf then set_parent (left y) x;
      set_parent y (parent x);
      if parent x = Leaf then t.root <- y
      else if x == left @@ parent x then set_child (parent x) Left y
      else set_child (parent x) Right y;
      set_child y Left x;
      set_bheight x @@ max (bheight @@ left x) (bheight @@ right x)
        + if get_node_colour x = Black then 1 else 0;
      set_bheight y @@ max (bheight @@ left y) (bheight @@ right y)
        + if get_node_colour y = Black then 1 else 0

    let rotate_right x t =
      let y = left x in
      set_child x Left (right y);
      if right y != Leaf then set_parent (right y) x;
      set_parent y (parent x);
      if parent x = Leaf then t.root <- y
      else if x == right @@ parent x then set_child (parent x) Right y
      else set_child (parent x) Left y;
      set_child y Right x;
      set_bheight x @@ max (bheight @@ left x) (bheight @@ right x)
        + if get_node_colour x = Black then 1 else 0;
      set_bheight y @@ max (bheight @@ left y) (bheight @@ right y)
        + if get_node_colour y = Black then 1 else 0

    let fix_tree n t =
      let prev_k = ref n in 
      let k = ref n in
      while get_node_colour (parent !k) = Red && parent (!k) <> Leaf && parent (parent (!k)) <> Leaf do
        if parent !k == right @@ parent @@ parent !k then
          let u = left @@ parent @@ parent !k in
          update_bheight u;
          update_bheight (parent !k);
          update_bheight (parent @@ parent !k);
          if get_node_colour u == Red then begin
            set_colour_and_update_bheight u Black;
            set_colour_and_update_bheight (parent !k) Black;
            set_colour (parent @@ parent !k) Red;
            k := parent @@ parent !k
          end
          else begin
            if !k == left @@ parent !k then begin
              k := parent !k;
              rotate_right !k t;
            end;
            set_colour_and_update_bheight (parent !k) Black;
            set_colour (parent @@ parent !k) Red;
            rotate_left (parent @@ parent !k) t
          end
        else
          let u = right @@ parent @@ parent !k in
          update_bheight u;
          update_bheight (parent !k);
          update_bheight (parent @@ parent !k);
          if get_node_colour u == Red then begin
            set_colour_and_update_bheight u Black;
            set_colour_and_update_bheight (parent !k) Black;
            set_colour (parent @@ parent !k) Red;
            k := parent @@ parent !k
          end
          else begin
            if !k == right @@ parent !k then begin
              k := parent !k;
              rotate_left !k t
            end;
            set_colour_and_update_bheight (parent !k) Black;
            set_colour (parent @@ parent !k) Red;
            rotate_right (parent @@ parent !k) t
          end;
        prev_k := !k;
      done;
      update_bheight t.root;
      set_colour_and_update_bheight t.root Black

    let rec insert_aux new_node current_node t =
      let k = get_node_key new_node in
      match current_node with
      | Leaf -> failwith "This is not supposed to happen"
      | Node n' ->
        if n'.key = k then ()
        else if n'.key > k then
          (if n'.left = Leaf then (set_child current_node Left new_node; fix_tree new_node t)
          else insert_aux new_node n'.left t)
        else if n'.right = Leaf then (set_child current_node Right new_node; fix_tree new_node t)
        else insert_aux new_node n'.right t

    let insert k v t =
      let new_node = new_node k v in
      match new_node with
      | Leaf -> failwith "Can't insert Leaf"
      | Node n ->
        match t.root with
        | Leaf -> (n.colour <- Black; n.bheight <- 2; n.parent <- Leaf; t.root <- new_node)
        | Node _ -> (n.colour <- Red; insert_aux new_node t.root t)

    let rec find_min_node n =
      match n with
      | Leaf -> failwith "Find min node function: n is a leaf"
      | Node n' ->
        if n'.left == Leaf then n
        else find_min_node (n'.left)
    
    (* let rec delete_aux current_node k t =
      if current_node == Leaf then ()
      else if k < key current_node then
        (delete_aux (left current_node) k t)
      else if key current_node < k then
        (delete_aux (right current_node) k t)
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
            n'.tval <- tval min_node;
            delete_aux n'.right (key min_node) t
      end *)

    (* let rec get_black_height_aux acc n =
      match n with
      | Leaf -> acc + 1
      | Node n' -> get_black_height_aux (acc + if n'.colour = Black then 1 else 0) n'.left

    let get_black_height n = get_black_height_aux 0 n *)

    let get_rank n = 
      let bh = bheight n in
      if get_node_colour n = Black then 2 * (bh - 1) else 2 * bh - 1

    let rec join_right tl n tr =
      if get_rank tl.root = int_of_float (floor @@ float_of_int (get_rank tr.root) /. 2.) * 2 then
        (set_colour n Red; merge_three_nodes tl.root n tr.root; {root = n})
      else begin
        let (l, mn, r) = expose tl.root in
        let c = get_node_colour mn in
        let ntr = join_right {root = r} n tr in
        merge_three_nodes l mn ntr.root;
        let t'' = {root = mn} in
        if c == Black && get_node_colour (right mn) = Red && get_node_colour (right @@ right mn) = Red then begin
          set_colour_and_update_bheight (right @@ right mn) Black;
          rotate_left mn t''
        end; t''
      end
      ;;

    let rec join_left tl n tr =
      if get_rank tr.root = int_of_float (floor @@ float_of_int (get_rank tl.root) /. 2.) * 2 then
        (set_colour n Red; merge_three_nodes tl.root n tr.root; {root = n})
      else begin
        let (l, mn, r) = expose tr.root in
        let c = get_node_colour mn in
        let ntl = join_left tl n {root = l} in
        merge_three_nodes ntl.root mn r;
        let t'' = {root = mn} in
        if c == Black && get_node_colour (left mn) = Red && get_node_colour (left @@ left mn) = Red then begin
          set_colour_and_update_bheight (left @@ left mn) Black;
          rotate_right mn t''
        end; t''
      end

    let join tl n tr =
      let ctl = int_of_float (floor @@ float_of_int (get_rank tl.root) /. 2.) in
      let ctr = int_of_float (floor @@ float_of_int (get_rank tr.root) /. 2.) in
      if ctl > ctr then
        let nt = join_right tl n tr in
        if get_node_colour nt.root = Red && get_node_colour (right nt.root) = Red then
          set_colour_and_update_bheight nt.root Black;
        nt
      else if ctr > ctl then
        let nt = join_left tl n tr in
        if get_node_colour nt.root = Red && get_node_colour (left nt.root) = Red then
          set_colour_and_update_bheight nt.root Black;
        nt
      else if get_node_colour tl.root = Black && get_node_colour tr.root = Black then
        (set_colour n Red; merge_three_nodes tl.root n tr.root; {root = n})
      else (set_colour n Black; merge_three_nodes tl.root n tr.root; {root = n})

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
          if get_node_colour n'.left = Black && get_node_colour n'.right = Black then
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
      if check_root_color then get_node_colour t.root = Black
      else
        let (bd, _) = verify_black_depth t.root in
        bd
        &&
        verify_internal_property t.root
  end

  type 'a t = 'a Sequential.rb_tree

  type ('a, 'b) op =
    | Insert : V.t * 'a -> ('a, unit) op
    | Search : V.t -> ('a, 'a option) op
    (* | Size : ('elt, int) op *)

  type 'a wrapped_op = Mk : ('a, 'b) op * ('b -> unit) -> 'a wrapped_op

  let init () = Sequential.new_tree ()

  (* let rec binary_search arr target left right =
    if left > right then
      match fst arr.(left) with
      | key when key >= target -> left
      | _ -> 0 (* No element greater than or equal to the target *)
    else
      let mid = (left + right) / 2 in
      match fst arr.(mid) with
      | key when key = target -> mid (* Found the target element *)
      | key when key < target -> binary_search arr target (mid + 1) right
      | _ -> binary_search arr target left (mid - 1) *)

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

  (* Section searches depending on node *)
  (* let rec par_search_aux search_threshold tree_threshold pool t ~keys ~range:(rstart, rstop) =
    let h = Sequential.bheight @@ Sequential.root_node t in
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n < search_threshold || h < tree_threshold then
      for i = rstart to rstop - 1 do
        let (k, kont) = keys.(i) in kont @@ Sequential.search k t
      done
    else 
      match Sequential.root_node t with
      | Sequential.Leaf -> for i = rstart to rstop - 1 do
          let (k, kont) = keys.(i) in kont @@ Sequential.search k t
        done
      | Node rn ->
        try begin
          let idx = binary_search keys rn.key rstart rstop in
          let i = ref idx in
          let ck = ref k in
          if k == Sequential.key @@ Sequential.root_node t then 
            while !ck == k && !i < rstop do
              let (k, kont) = keys.(!i) in kont @@ Sequential.search k t;
              i := !i + 1; ck := k
            done;
          let _ = Domainslib.Task.async pool 
            (fun () -> par_search_aux
              search_threshold tree_threshold pool
              {root = Sequential.right @@ Sequential.root_node t} ~keys ~range:(!i, rstop)) in
          let _ = Domainslib.Task.async pool 
            (fun () -> par_search_aux
              search_threshold tree_threshold pool
              {root = Sequential.left @@ Sequential.root_node t} ~keys ~range:(rstart, !i)) in ()
        end with _ -> for i = rstart to rstop - 1 do
          let (_, kont) = keys.(i) in kont None
        done *)

  (* let rec par_search_aux op_threshold height_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if node = Sequential.Leaf then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ Sequential.search_aux k node done
      (* Domainslib.Task.parallel_for pool ~start:rstart ~finish:(rstop - 1) ~body:(fun i ->
        let (k,kont) = keys.(i) in
        kont @@ Sequential.search_aux k node) *)
    else
      let k = Sequential.key node in
      let nval = Sequential.tval node in
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
      (* let split = binary_search keys k rstart rstop in
      let s1 = ref split and s2 = ref split in
      while !s1 > rstart && fst keys.(!s1 - 1) = k do
        s1 := !s1 - 1;
        snd keys.(!s1) @@ Some nval;
      done;
      while !s2 < rstop && fst keys.(!s2) = k do
        snd keys.(!s2) @@ Some nval;
        s2 := !s2 + 1
      done; *)
      (* let lt = Sequential.new_tree_with_node @@ Sequential.left n in
      let rt = Sequential.new_tree_with_node @@ Sequential.right n in *)
      let _ = Domainslib.Task.async pool 
        (fun () -> par_search_aux op_threshold height_threshold ~pool (Sequential.left node) ~keys ~range:(rstart, !s1)) in
      let _ = Domainslib.Task.async pool
        (fun () -> par_search_aux op_threshold height_threshold ~pool (Sequential.right node) ~keys ~range:(!s2, rstop)) in ()

  let par_search ?search_threshold ?tree_threshold ~pool (t: 'a t) keys =
    let search_threshold = match search_threshold with Some t -> t | None -> !rbtree_search_sequential_threshold in
    let tree_threshold = match tree_threshold with Some t -> t | None -> !rbtree_search_height_threshold in
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> V.compare k k') keys;
    par_search_aux search_threshold tree_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)

  (* let rec par_search_aux threshold pool t ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n > threshold then
      let num_par = n / threshold + if n mod threshold > 0 then 1 else 0 in
      Domainslib.Task.parallel_for pool ~start:0 ~finish:(num_par - 1) ~body:(fun i ->
        par_search_aux threshold pool t ~keys ~range:(rstart + i * threshold, min rstop @@ rstart + (i + 1) * threshold)
      );
    else
      for i = rstart to rstop - 1 do
        let (k, kont) = keys.(i) in kont @@ Sequential.search k t
      done *)

  (* let par_search ?threshold ~pool (t: 'a t) keys =
    let threshold = match threshold with Some t -> t | None -> !rbtree_search_sequential_threshold in
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> V.compare k k') keys;
    par_search_aux threshold pool t ~keys ~range:(0, Array.length keys) *)

  (* let rec par_insert_aux threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= threshold then
      for i = rstart to rstop - 1 do
        let (k, v) = inserts.(i) in
        let n = Sequential.new_node k v in
        Sequential.insert n t
      done
    else
      let mid = rstart + n / 2 in
      let (mk, nv) = inserts.(mid) in
      let (lt, mn, rt) = Sequential.split t mk in
      let nn = match mn with
      | Leaf -> Sequential.new_node mk nv 
      | Node _ -> mn in
      let l = Domainslib.Task.async pool 
        (fun () -> par_insert_aux threshold ~pool lt ~inserts ~range:(rstart, mid)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux threshold ~pool rt ~inserts ~range:(mid + 1, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      let (nlt, _, _) = Sequential.split lt (Sequential.get_node_key nn) in (* Make sure there's no duplicate *)
      let (_, _, nrt) = Sequential.split rt (Sequential.get_node_key nn) in
      let nt = Sequential.join nlt nn nrt in
      t.root <- nt.root

  let par_insert ?threshold ~pool (t: 'a t) inserts =
    let threshold = match threshold with Some t -> t | None -> !rbtree_insert_sequential_threshold in
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> V.compare k k') inserts;
    par_insert_aux threshold ~pool t ~inserts ~range:(0, Array.length inserts) *)
  
let rec par_insert_aux op_threshold height_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
  let n = rstop - rstart in
  if n <= 0 then ()
  else if n <= op_threshold || Sequential.bheight (Sequential.root_node t) <= height_threshold then
    for i = rstart to rstop - 1 do
      let (k, v) = inserts.(i) in
      Sequential.insert k v t
    done
  else
    let (ln, mn, rn) = Sequential.expose @@ Sequential.root_node t in
    let lt = Sequential.new_tree_with_node ln and rt = Sequential.new_tree_with_node rn in
    let k = Sequential.key mn in
    let split = binary_search inserts k rstart rstop in
    (* let s1 = ref split and s2 = ref split in
    while !s1 > rstart && fst inserts.(!s1 - 1) = k do s1 := !s1 - 1 done;
    while fst inserts.(!s2) = k && !s2 >= rstop do s2 := !s2 + 1 done; *)
    let l = Domainslib.Task.async pool 
      (fun () -> par_insert_aux op_threshold height_threshold ~pool lt ~inserts ~range:(rstart, split)) in
    let r = Domainslib.Task.async pool
      (fun () -> par_insert_aux op_threshold height_threshold ~pool rt ~inserts ~range:(split, rstop)) in
    Domainslib.Task.await pool l; Domainslib.Task.await pool r;
    let (nlt, _, _) = Sequential.split lt (Sequential.key mn) in (* Make sure there's no duplicate *)
    let (_, _, nrt) = Sequential.split rt (Sequential.key mn) in
    let nt = Sequential.join nlt mn nrt in
    (* let nt = Sequential.merge_three_nodes lt.root mn rt.root in *)
    t.root <- nt.root
  
  let par_insert ?op_threshold ?height_threshold ~pool (t: 'a t) inserts =
    let op_threshold = match op_threshold with Some t -> t | None -> !rbtree_insert_sequential_threshold in
    let height_threshold = match height_threshold with Some t -> t | None -> !rbtree_insert_height_threshold in
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> V.compare k k') inserts;
    par_insert_aux op_threshold height_threshold ~pool t ~inserts ~range:(0, Array.length inserts) *)

  (** Use both binary search and linear search to traverse operations array *)
  let rec par_search_aux_4 op_threshold height_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if node = Sequential.Leaf then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    (* else if n <= op_threshold || Sequential.height node <= height_threshold then *)
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ Sequential.search_aux k node done 
      (* Domainslib.Task.parallel_for pool ~start:rstart ~finish:(rstop - 1) ~body:(fun i ->
        let (k,kont) = keys.(i) in
        kont @@ Sequential.search_aux k node) *)
    else
      let k = Sequential.key node in
      let nval = Sequential.tval node in
      let s1 = ref rstart and s2 = ref rstart in
      if n > !rbtree_binary_search_threshold then begin
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
        (fun () -> par_search_aux_4 op_threshold height_threshold ~pool (Sequential.left node) ~keys ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_4 op_threshold height_threshold ~pool (Sequential.right node) ~keys ~range:(!s2, rstop)) in
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
  let rec par_search_aux_2 op_threshold height_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if node = Sequential.Leaf then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ Sequential.search_aux k node done 
    else
      let k = Sequential.key node in
      let nval = Sequential.tval node in
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
        (fun () -> par_search_aux_2 op_threshold height_threshold ~pool (Sequential.left node) ~keys ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_2 op_threshold height_threshold ~pool (Sequential.right node) ~keys ~range:(!s2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r

  (** Use linear search only to traverse operations array *)
  let rec par_search_aux_3 op_threshold height_threshold ~pool node ~keys ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if node = Sequential.Leaf then
      for i = rstart to rstop - 1 do let (_,kont) = keys.(i) in kont None done
    else if n <= op_threshold then
      for i = rstart to rstop - 1 do let (k,kont) = keys.(i) in kont @@ Sequential.search_aux k node done 
    else
      let k = Sequential.key node in
      let nval = Sequential.tval node in
      let s1 = ref rstart and s2 = ref rstart in
      while !s1 < rstop && fst keys.(!s1) < k do s1 := !s1 + 1 done;
      s2 := !s1;
      while !s2 < rstop && fst keys.(!s2) = k do
        snd keys.(!s2) (Some nval);
        s2 := !s2 + 1
      done;
      let l = Domainslib.Task.async pool 
        (fun () -> par_search_aux_3 op_threshold height_threshold ~pool (Sequential.left node) ~keys ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_search_aux_3 op_threshold height_threshold ~pool (Sequential.right node) ~keys ~range:(!s2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r

  let par_search ?search_threshold ?tree_threshold ~pool (t: 'a t) keys =
    let search_threshold = match search_threshold with Some t -> t | None -> !rbtree_search_sequential_threshold in
    let tree_threshold = match tree_threshold with Some t -> t | None -> !rbtree_search_height_threshold in
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> V.compare k k') keys;
    match !rbtree_search_type with
    | 0 -> Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length keys - 1) ~body:(fun i ->
      let (k,kont) = keys.(i) in
      kont @@ Sequential.search k t)
    | 1 -> par_search_aux_1 search_threshold pool t ~keys ~range:(0, Array.length keys)
    | 2 -> par_search_aux_2 search_threshold tree_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)
    | 3 -> par_search_aux_3 search_threshold tree_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)
    | 4 -> par_search_aux_4 search_threshold tree_threshold ~pool (Sequential.root_node t) ~keys ~range:(0, Array.length keys)
    | _ -> failwith "Invalid search type"

  (** Mix of binary and linear search in partitioning insert array *)
  let rec par_insert_aux_3 op_threshold height_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || Sequential.bheight (Sequential.root_node t) <= height_threshold then
      for i = rstart to rstop - 1 do
        let (k, v) = inserts.(i) in
        Sequential.insert k v t
      done
    else
      let (ln, mn, rn) = Sequential.expose @@ Sequential.root_node t in
      let lt = Sequential.new_tree_with_node ln and rt = Sequential.new_tree_with_node rn in
      let s1 = ref rstart and s2 = ref rstart in
      if n > !rbtree_binary_search_threshold then begin
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
        (fun () -> par_insert_aux_3 op_threshold height_threshold ~pool lt ~inserts ~range:(rstart, !s1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_3 op_threshold height_threshold ~pool rt ~inserts ~range:(!s2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      let nt = Sequential.join lt mn rt in
      t.root <- nt.root

  (** Linear traversal of inserts *)
  let rec par_insert_aux_2 op_threshold height_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || Sequential.bheight (Sequential.root_node t) <= height_threshold then
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
        (fun () -> par_insert_aux_2 op_threshold height_threshold ~pool lt ~inserts ~range:(rstart, !mid1)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_2 op_threshold height_threshold ~pool rt ~inserts ~range:(!mid2, rstop)) in
      Domainslib.Task.await pool l; Domainslib.Task.await pool r;
      let nt = Sequential.join lt mn rt in
      t.root <- nt.root

  (* Use binary search on the sorted inserts array *)
  let rec par_insert_aux_1 op_threshold height_threshold ~pool (t: 'a t) ~inserts ~range:(rstart, rstop) =
    let n = rstop - rstart in
    if n <= 0 then ()
    else if n <= op_threshold || Sequential.bheight (Sequential.root_node t) <= height_threshold then
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
        (fun () -> par_insert_aux_1 op_threshold height_threshold ~pool lt ~inserts ~range:(rstart, split)) in
      let r = Domainslib.Task.async pool
        (fun () -> par_insert_aux_1 op_threshold height_threshold ~pool rt ~inserts ~range:(split, rstop)) in
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
    let threshold = match threshold with Some t -> t | None -> !rbtree_insert_sequential_threshold in
    Sort.sort pool ~compare:(fun (k, _) (k', _) -> V.compare k k') inserts;
    match !rbtree_insert_type with
    | 0 -> par_insert_aux_0 threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 1 -> par_insert_aux_1 threshold !rbtree_insert_height_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 2 -> par_insert_aux_2 threshold !rbtree_insert_height_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
    | 3 -> par_insert_aux_3 threshold !rbtree_insert_height_threshold ~pool t ~inserts ~range:(0, Array.length inserts)
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
      par_insert ~pool t inserts;
    end
end