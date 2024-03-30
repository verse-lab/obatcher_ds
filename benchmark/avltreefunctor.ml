module IntAvltree = Obatcher_ds.Avlgeneral.Sequential(Int)
module IntAvltreePrebatch = Obatcher_ds.Avlgeneral.Prebatch(Int)
module IntAvltreeSplitJoin = Obatcher_ds.Splitjoin.Make(IntAvltreePrebatch)
module BatchedIntAvltree = Domainslib.Batcher.Make1(IntAvltreeSplitJoin)
(* module IntAvltree = Obatcher_ds.Avl.Sequential(Int)
module IntAvltreePrebatch = Obatcher_ds.Avl.Prebatch(Int)
module IntAvltreeSplitJoin = Obatcher_ds.Binarysplitjoin.Make(IntAvltreePrebatch)
module BatchedIntAvltree = Domainslib.Batcher.Make1(IntAvltreeSplitJoin) *)
(* module IntAvltreeSplitJoin = Obatcher_ds.Binarysplitjoin.Make(Obatcher_ds.Avl.Prebatch(Int))
module IntAvltree = Obatcher_ds.Binarysplitjoin.Make(Obatcher_ds.Avlfunctor.AvlTree(Int))
module BatchedIntAvltree = Domainslib.Batcher.Make1(IntAvltree) *)

type generic_spec_args = {
  sorted: bool;
  no_searches: int;
  min: int;
  max: int;
  initial_count: int;
  should_validate: bool;
  search_threshold: int option;
  insert_threshold: int option;
  search_type: int option;
  insert_type: int option;
}

type generic_test_spec = {
  args: generic_spec_args;
  count: int;
  mutable initial_elements: int array;
  mutable insert_elements: int array;
  mutable search_elements: int array;
}

let generic_spec_args: generic_spec_args Cmdliner.Term.t =
  let open Cmdliner in
  let sorted = Arg.(value @@ flag  @@ info ~doc:"whether the inserts should be sorted" ["s"; "sorted"]) in
  let no_searches =
    Arg.(value @@ opt (some int) None @@ info ~doc:"number of searches" ~docv:"NO_SEARCHES" ["n"; "no-searches"]) in
  let initial_count =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Initial number of operations" ["init-count"]) in
  let min =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Minimum value of data for random inputs" ["min"]) in
  let max =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Maximum value of data for random inputs" ["max"]) in
  let validate =
    Arg.(value @@ flag @@ info ~doc:"Whether the tests should validate the results of the benchmarks" ["T"]) in
  let search_threshold =
    Arg.(value @@ opt (some int) None @@
         info ~doc:"Threshold upon which searches should be sequential" ["search-threshold"]) in
  let insert_threshold =
    Arg.(value @@ opt (some int) None @@
         info ~doc:"Threshold upon which inserts should be sequential" ["insert-threshold"]) in
  let insert_type =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Which parallel insert to use" ["insert-type"]) in
  let search_type =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Which parallel search to use" ["search-type"]) in

  Term.(const (fun sorted no_searches min max
                initial_count validate search_threshold insert_threshold search_type insert_type
                 -> {
      sorted;
      no_searches=Option.value ~default:0 no_searches;
      initial_count=Option.value ~default:0 initial_count;
      min=Option.value ~default:0 min;
      max=Option.value ~default:((Int.shift_left 1 30) - 1) max;
      should_validate=validate;
      search_threshold;
      insert_threshold;
      search_type;
      insert_type;
    }) $ sorted $ no_searches $ min $ max $ initial_count $
        validate $ search_threshold $ insert_threshold $ search_type $ insert_type)

let generic_test_spec ~count spec_args =
  { args=spec_args; count: int; insert_elements=[| |]; search_elements=[| |]; initial_elements=[| |] }

let generic_run test_spec f =
  let old_search_threshold = !IntAvltreeSplitJoin.search_op_threshold in
  let old_insert_threshold = !IntAvltreeSplitJoin.insert_op_threshold in
  let old_search_type = !IntAvltreeSplitJoin.search_type in
  let old_insert_type = !IntAvltreeSplitJoin.insert_type in
  (match test_spec.args.search_threshold with None -> () | Some st ->
    IntAvltreeSplitJoin.search_op_threshold := st);
  (match test_spec.args.insert_threshold with None -> () | Some it ->
    IntAvltreeSplitJoin.insert_op_threshold := it);
  (match test_spec.args.search_type with None -> () | Some it ->
    IntAvltreeSplitJoin.search_type := it);
  (match test_spec.args.insert_type with None -> () | Some it ->
    IntAvltreeSplitJoin.insert_type := it);
  let res = f () in
  IntAvltreeSplitJoin.search_op_threshold := old_search_threshold;
  IntAvltreeSplitJoin.insert_op_threshold := old_insert_threshold;
  IntAvltreeSplitJoin.search_type := old_search_type;
  IntAvltreeSplitJoin.insert_type := old_insert_type;
  res
  
let generic_init test_spec f =
  let min, max =  test_spec.args.min, test_spec.args.max in
  let elements = Util.gen_random_unique_array ~min ~max (test_spec.args.initial_count + test_spec.count) in
  let initial_elements = Array.make test_spec.args.initial_count min in
  let insert_elements = Array.make test_spec.count min in
  let search_elements = Util.gen_random_unique_array ~min ~max test_spec.args.no_searches in
  Array.blit
    elements 0
    initial_elements 0
    test_spec.args.initial_count;
  Array.blit
    elements test_spec.args.initial_count
    insert_elements 0
    test_spec.count;
  if test_spec.args.sorted then
    Array.sort Int.compare insert_elements;
  test_spec.insert_elements <- insert_elements;
  test_spec.initial_elements <- initial_elements;
  test_spec.search_elements <- search_elements;
  generic_run test_spec @@ fun () -> f initial_elements 

module Sequential = struct

  type t = unit IntAvltree.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec =
    generic_init test_spec (fun initial_elements ->
      let tree = IntAvltree.init () in
      Array.iter (fun i -> IntAvltree.insert i () tree)
        initial_elements;
      tree
    )

  let run _pool t test_spec =
    generic_run test_spec @@ fun () -> 
    Array.iter (fun i ->
        IntAvltree.insert i () t
      ) test_spec.insert_elements;
    Array.iter (fun i ->
        ignore @@ IntAvltree.search i t
      ) test_spec.search_elements

  let cleanup (t: t) (test_spec: test_spec) = 
    if test_spec.args.should_validate then begin
      Array.iter (fun elt ->
        match IntAvltree.search elt t with
        | Some _ -> ()
        | None -> Format.ksprintf failwith "Could not find inserted element %d in tree" elt
      ) test_spec.insert_elements;
    end
end

module CoarseGrained = struct

  type t = {tree: unit IntAvltree.t; mutex: Mutex.t}

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec =
    generic_init test_spec (fun initial_elements ->
      let tree = IntAvltree.init () in
      Array.iter (fun i -> IntAvltree.insert i () tree)
        initial_elements;
      let mutex = Mutex.create () in
      {tree;mutex}
    )

  let run pool (t: t) test_spec =
    generic_run test_spec @@ fun () ->
    Domainslib.Task.parallel_for pool ~chunk_size:1
      ~start:0 ~finish:(Array.length test_spec.insert_elements + Array.length test_spec.search_elements - 1)
      ~body:(fun i ->
          Mutex.lock t.mutex;
          Fun.protect ~finally:(fun () -> Mutex.unlock t.mutex) (fun () ->
              if i < Array.length test_spec.insert_elements
              then IntAvltree.insert test_spec.insert_elements.(i) () t.tree
              else ignore (IntAvltree.search
                             test_spec.search_elements.(i - Array.length test_spec.insert_elements) t.tree)
            )
        )

  let cleanup (t: t) (test_spec: test_spec) =
    if test_spec.args.should_validate then begin
      Array.iter (fun elt ->
        match IntAvltree.search elt t.tree with
        | Some _ -> ()
        | None -> Format.ksprintf failwith "Could not find inserted element %d in tree" elt
      ) test_spec.insert_elements
    end

end

module Batched = struct

  type t = unit BatchedIntAvltree.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init pool test_spec =
    generic_init test_spec (fun initial_elements ->
      let tree = BatchedIntAvltree.init pool in
      let exposed_tree = BatchedIntAvltree.unsafe_get_internal_data tree in
      Array.iter (fun i -> IntAvltree.insert i () exposed_tree) initial_elements;
      BatchedIntAvltree.restart_batcher_timer tree;
      tree)

  let run pool (tree: t) test_spec =
    generic_run test_spec @@ fun () -> 
    Domainslib.Task.parallel_for pool ~chunk_size:1
      ~start:0 ~finish:(Array.length test_spec.insert_elements + Array.length test_spec.search_elements - 1)
      ~body:(fun i ->
        if i < Array.length test_spec.insert_elements
        then BatchedIntAvltree.apply tree (Insert (test_spec.insert_elements.(i), ()))
        else 
          ignore (BatchedIntAvltree.apply tree (Search test_spec.search_elements.(i - Array.length test_spec.insert_elements)))
      );
    BatchedIntAvltree.wait_for_batch tree

  let cleanup (t: t) (test_spec: test_spec) =
    if test_spec.args.should_validate then begin
      let t = BatchedIntAvltree.unsafe_get_internal_data t in
      let num_nodes = IntAvltree.num_nodes t in
      if num_nodes <> Array.length test_spec.insert_elements + Array.length test_spec.initial_elements
        then Format.ksprintf failwith "Inserted %d elements, but found only %d in the tree"
      (Array.length test_spec.insert_elements + Array.length test_spec.initial_elements)
      num_nodes;
      let btree_flattened = IntAvltree.flatten t |> Array.of_list in
      let all_elements = Array.concat [test_spec.insert_elements; test_spec.initial_elements] in
      Array.sort Int.compare all_elements;
      if Array.length btree_flattened <> Array.length all_elements then
      Format.ksprintf failwith "length of flattened btree (%d) did not match inserts (%d) (no_elements=%d)"
        (Array.length btree_flattened) (Array.length all_elements) (num_nodes);

      for i = 0 to Array.length btree_flattened - 1 do
        if fst btree_flattened.(i) <> all_elements.(i) then
          Format.ksprintf failwith "element %d of the btree was expected to be %d, but got %d" i
            all_elements.(i) (fst btree_flattened.(i));
      done;

      Array.iter (fun elt ->
        match IntAvltree.search elt t with
        | Some _ -> ()
        | None -> Format.ksprintf failwith "Could not find inserted element %d in tree" elt
      ) test_spec.insert_elements;
    end

end

module ExplicitlyBatched = struct

  type t = unit IntAvltree.t

  type test_spec = {
    spec: generic_test_spec;
    mutable insert_elements: (int * unit) array;
    mutable search_elements: (int * (unit option -> unit)) array;
  }

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    let spec = generic_test_spec ~count spec_args in
    {spec; insert_elements=[||]; search_elements=[||]}

  let init _pool (test_spec: test_spec) =
    generic_init test_spec.spec (fun initial_elements ->
      test_spec.insert_elements <- Array.map (fun i -> (i, ())) test_spec.spec.insert_elements;
      test_spec.search_elements <- Array.map (fun i -> (i, (fun _ -> ()))) test_spec.spec.search_elements;
      let tree = IntAvltree.init () in
      Array.iter (fun i -> IntAvltree.insert i () tree)
        initial_elements;
      tree)

  let run pool (tree: t) test_spec =
    generic_run test_spec.spec @@ fun () -> 
    if Array.length test_spec.insert_elements > 0 then
      IntAvltreeSplitJoin.par_insert ~pool tree test_spec.insert_elements;
    if Array.length test_spec.spec.search_elements > 0 then
      ignore @@ IntAvltreeSplitJoin.par_search ~pool tree test_spec.search_elements


  let cleanup (t: t) (test_spec: test_spec) =
    if test_spec.spec.args.should_validate then begin
      let num_nodes = IntAvltree.num_nodes t in
      if num_nodes <> Array.length test_spec.insert_elements + Array.length test_spec.spec.initial_elements
        then Format.ksprintf failwith "Inserted %d elements, but found only %d in the tree"
      (Array.length test_spec.insert_elements + Array.length test_spec.spec.initial_elements)
      num_nodes;
      let btree_flattened = IntAvltree.flatten t |> Array.of_list in
      let all_elements = Array.concat [test_spec.spec.insert_elements; test_spec.spec.initial_elements] in
      Array.sort Int.compare all_elements;
      if Array.length btree_flattened <> Array.length all_elements then
      Format.ksprintf failwith "length of flattened btree (%d) did not match inserts (%d) (no_elements=%d)"
        (Array.length btree_flattened) (Array.length all_elements) (num_nodes);

      for i = 0 to Array.length btree_flattened - 1 do
        if fst btree_flattened.(i) <> all_elements.(i) then
          Format.ksprintf failwith "element %d of the btree was expected to be %d, but got %d" i
            all_elements.(i) (fst btree_flattened.(i));
      done;

      Array.iter (fun elt ->
        match IntAvltree.search elt t with
        | Some _ -> ()
        | None -> Format.ksprintf failwith "Could not find inserted element %d in tree" elt
      ) test_spec.spec.insert_elements;
    end

end
