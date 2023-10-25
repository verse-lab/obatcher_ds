module IntSet = Set.Make(Int)
module IntBtree = Obatcher_ds.Btree.Make(Int)
module BatchedIntBtree = Domainslib.Batcher.Make1(IntBtree)

type generic_spec_args = {
  sorted: bool;
  no_searches: int;
  min: int;
  max: int;
  initial_count: int;
  should_validate: bool;
  search_threshold: int option;
  search_par_threshold: int option;
  insert_threshold: int option;
  branching_factor: int option;
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
  let search_par_threshold =
    Arg.(value @@ opt (some int) None @@
         info ~doc:"Threshold upon which searches should be done in parallel" ["search-par-threshold"]) in

  let insert_threshold =
    Arg.(value @@ opt (some int) None @@
         info ~doc:"Threshold upon which searches should be sequential" ["insert-threshold"]) in
  let branching_factor =
    Arg.(value @@ opt (some int) None @@
         info ~doc:"Branching factor of tree" ["branching-factor"]) in

  Term.(const (fun sorted no_searches min max
                initial_count validate search_threshold search_par_threshold insert_threshold
                branching_factor -> {
      sorted;
      no_searches=Option.value ~default:0 no_searches;
      initial_count=Option.value ~default:0 initial_count;
      min=Option.value ~default:0 min;
      max=Option.value ~default:((Int.shift_left 1 30) - 1) max;
      should_validate=validate;
      search_threshold;
      search_par_threshold;
      insert_threshold;
      branching_factor
    }) $ sorted $ no_searches $ min $ max $ initial_count $
        validate $ search_threshold $ search_par_threshold $ insert_threshold $ branching_factor)

let generic_test_spec ~count spec_args =
  { args=spec_args; count: int; insert_elements=[| |]; search_elements=[| |]; initial_elements=[| |] }

let generic_run test_spec f =
  let old_search_threshold = !Obatcher_ds.Btree.btree_search_sequential_threshold in
  let old_search_par_threshold = !Obatcher_ds.Btree.btree_search_parallel_threshold in
  let old_insert_threshold = !Obatcher_ds.Btree.btree_search_sequential_threshold in
  let old_branching_factor = !Obatcher_ds.Btree.btree_max_children in
  Obatcher_ds.Btree.btree_search_sequential_threshold := test_spec.args.search_threshold;
  Obatcher_ds.Btree.btree_search_parallel_threshold := test_spec.args.search_par_threshold;
  Obatcher_ds.Btree.btree_insert_sequential_threshold := test_spec.args.insert_threshold;
  Option.iter (fun vl -> Obatcher_ds.Btree.btree_max_children := vl)
    test_spec.args.branching_factor;
  let res = f () in
  Obatcher_ds.Btree.btree_search_sequential_threshold := old_search_threshold;
  Obatcher_ds.Btree.btree_search_parallel_threshold := old_search_par_threshold;
  Obatcher_ds.Btree.btree_insert_sequential_threshold := old_insert_threshold;
  Obatcher_ds.Btree.btree_max_children := old_branching_factor;
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

  type t = unit IntBtree.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec =
    generic_init test_spec (fun initial_elements ->
      let tree = IntBtree.Sequential.init () in
      Array.iter (fun i -> IntBtree.Sequential.insert tree i ())
        initial_elements;
      tree
    )


  let run _pool t test_spec =
    generic_run test_spec @@ fun () -> 
    Array.iter (fun i ->
        IntBtree.Sequential.insert t i ()
      ) test_spec.insert_elements;
    Array.iter (fun i ->
        ignore @@ IntBtree.Sequential.search t i
      ) test_spec.search_elements

  let cleanup (t: t) (test_spec: test_spec) =
    if test_spec.args.should_validate then begin
      Array.iter (fun elt ->
        match IntBtree.Sequential.search t elt with
        | Some _ -> ()
        | None -> Format.ksprintf failwith "Could not find inserted element %d in tree" elt
      ) test_spec.insert_elements
    end
    
end


module CoarseGrained = struct

  type t = {tree: unit IntBtree.t; mutex: Mutex.t}

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec =
    generic_init test_spec (fun initial_elements ->
      let tree = IntBtree.Sequential.init () in
      Array.iter (fun i -> IntBtree.Sequential.insert tree i ())
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
              then IntBtree.Sequential.insert t.tree test_spec.insert_elements.(i) ()
              else ignore (IntBtree.Sequential.search t.tree
                             test_spec.search_elements.(i - Array.length test_spec.insert_elements))
            )
        )

  let cleanup (t: t) (test_spec: test_spec) =
    if test_spec.args.should_validate then begin
      Array.iter (fun elt ->
        match IntBtree.Sequential.search t.tree elt with
        | Some _ -> ()
        | None -> Format.ksprintf failwith "Could not find inserted element %d in tree" elt
      ) test_spec.insert_elements
    end

end


module Batched = struct

  type t = unit BatchedIntBtree.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init pool test_spec =
    generic_init test_spec (fun initial_elements ->
      let tree = BatchedIntBtree.init pool in
      Array.iter (fun i -> BatchedIntBtree.apply tree (Insert (i, ())))
        initial_elements;
      tree)

  let run pool (tree: t) test_spec =
    generic_run test_spec @@ fun () -> 
    Domainslib.Task.parallel_for pool ~chunk_size:1
      ~start:0 ~finish:(Array.length test_spec.insert_elements + Array.length test_spec.search_elements - 1)
      ~body:(fun i ->
        if i < Array.length test_spec.insert_elements
        then BatchedIntBtree.apply tree (Insert (test_spec.insert_elements.(i), ()))
        else 
          ignore (BatchedIntBtree.apply tree (Search test_spec.search_elements.(i - Array.length test_spec.insert_elements)))
      );
    BatchedIntBtree.wait_for_batch tree

    
  let cleanup (t: t) (test_spec: test_spec) =
    let t = BatchedIntBtree.unsafe_get_internal_data t in
    if test_spec.args.should_validate then begin
      if t.IntBtree.Sequential.root.no_elements <> Array.length test_spec.insert_elements + Array.length test_spec.initial_elements
      then Format.ksprintf failwith "Inserted %d elements, but found only %d in the tree"
             (Array.length test_spec.insert_elements + Array.length test_spec.initial_elements)
             t.IntBtree.Sequential.root.no_elements;
      let btree_flattened = IntBtree.flatten t.root |> Array.of_seq in
      let all_elements = Array.concat [test_spec.insert_elements; test_spec.initial_elements] in
      Array.sort Int.compare all_elements;
      if Array.length btree_flattened <> Array.length all_elements then
        Format.ksprintf failwith "length of flattened btree (%d) did not match inserts (%d) (no_elements=%d)"
          (Array.length btree_flattened) (Array.length all_elements) (t.root.no_elements);

      for i = 0 to Array.length btree_flattened - 1 do
        if fst btree_flattened.(i) <> all_elements.(i) then
          Format.ksprintf failwith "element %d of the btree was expected to be %d, but got %d" i
            all_elements.(i) (fst btree_flattened.(i));
      done;

      Array.iter (fun elt ->
        match IntBtree.Sequential.search t elt with
        | Some _ -> ()
        | None -> Format.ksprintf failwith "Could not find inserted element %d in tree" elt
      ) test_spec.insert_elements;
    end;

end


module ExplicitlyBatched = struct

  type t = unit IntBtree.t

  type test_spec = {
    spec: generic_test_spec;
    mutable sorted_insert_elements: (int * unit) array;
    mutable search_elements: (int * (unit option -> unit)) array;
  }

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    let spec = generic_test_spec ~count spec_args in
    {spec; sorted_insert_elements=[||]; search_elements=[||]}

  let init _pool (test_spec: test_spec) =
    generic_init test_spec.spec (fun initial_elements ->
      let tree = IntBtree.Sequential.init () in
      test_spec.sorted_insert_elements <- Array.map (fun i -> (i, ())) test_spec.spec.insert_elements;
      test_spec.search_elements <- Array.map (fun i -> (i, (fun _ -> ()))) test_spec.spec.search_elements;
      Array.sort (fun (k1,_) (k2, _) -> Int.compare k1 k2) test_spec.sorted_insert_elements;
      Array.iter (fun i -> IntBtree.Sequential.insert tree i ())
        initial_elements;
      tree)

  let run pool (tree: t) test_spec =
    generic_run test_spec.spec @@ fun () -> 
    if Array.length test_spec.sorted_insert_elements > 0 then
      IntBtree.par_insert ~can_rebuild:false ~pool tree test_spec.sorted_insert_elements;
    if Array.length test_spec.spec.search_elements > 0 then
      ignore @@ IntBtree.par_search ~pool tree test_spec.search_elements

  let cleanup (t: t) (test: test_spec) =
    if test.spec.args.should_validate then begin
      if t.IntBtree.Sequential.root.no_elements <> Array.length test.spec.insert_elements + Array.length test.spec.initial_elements
      then Format.ksprintf failwith "Inserted %d elements, but found only %d in the tree"
             (Array.length test.spec.insert_elements + Array.length test.spec.initial_elements)
             t.IntBtree.Sequential.root.no_elements;
      let btree_flattened = IntBtree.flatten t.root |> Array.of_seq in
      let all_elements = Array.concat [test.spec.insert_elements; test.spec.initial_elements] in
      Array.sort Int.compare all_elements;
      if Array.length btree_flattened <> Array.length all_elements then
        Format.ksprintf failwith "length of flattened btree (%d) did not match inserts (%d) (no_elements=%d)"
          (Array.length btree_flattened) (Array.length all_elements) (t.root.no_elements);

      for i = 0 to Array.length btree_flattened - 1 do
        if fst btree_flattened.(i) <> all_elements.(i) then
          Format.ksprintf failwith "element %d of the btree was expected to be %d, but got %d" i
            all_elements.(i) (fst btree_flattened.(i));
      done;

      Array.iter (fun elt ->
        match IntBtree.Sequential.search t elt with
        | Some _ -> ()
        | None -> Format.ksprintf failwith "Could not find inserted element %d in tree" elt
      ) test.spec.insert_elements;
    end

end

