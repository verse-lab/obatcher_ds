[@@@warning "-32-26-27"]

module IntYfastTrie = Obatcher_ds.Yfast.Sequential
module IntYfastTriePrebatch = Obatcher_ds.Yfast.Prebatch
module IntYfastTrieFunctor = Obatcher_ds.Exposerepair.Make(IntYfastTriePrebatch)
module BatchedYfastTrie = Domainslib.Batcher.Make(IntYfastTrieFunctor)
(* module ExplicitlyBatchedSkiplist = Data.Skiplist.Make(Int)
module LazySkiplist = Data.Lazy_slist.Make(struct include Int let hash t = t end) *)

let int_size = 24

type generic_test_spec = {
  initial_elements: int array;
  insert_elements : int array;
  search_elements : int array;
  (* size : int *)
}

type generic_spec_args = {
  sorted : bool;
  no_searches : int;
  (* no_size : int; *)
  initial_count: int;
  min: int;
  max: int;
}

let generic_spec_args: generic_spec_args Cmdliner.Term.t =
  let open Cmdliner in
  let sorted = Arg.(value @@ flag  @@ info ~doc:"whether the inserts should be sorted" ["s"; "sorted"]) in
  let no_searches =
    Arg.(value @@ opt (some int) None @@ info ~doc:"number of searches" ~docv:"NO_SEARCHES" ["n"; "no-searches"]) in
  (* let no_size = 
    Arg.(value @@ opt (some int) None @@ info ~doc:"number of size operation calls" ~docv:"NO_SIZE" ["n_sz"; "no-size"]) in *)
  let initial_count =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Initial number of operations" ["init-count"]) in
  let min =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Minimum value of data for random inputs" ["min"]) in
  let max =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Maximum value of data for random inputs" ["max"]) in
  Term.(const (fun sorted no_searches initial_count min max ->
    {
      sorted;
      no_searches=Option.value ~default:0 no_searches;
      (* no_size=Option.value ~default:0 no_size; *)
      initial_count=Option.value ~default:0 initial_count;
      min=Option.value ~default:0 min;
      (* max=Option.value ~default:((Int.shift_left 1 30) - 1) max; *)
      max=Option.value ~default:(1 lsl int_size - 1) max;
    }) $ sorted $ no_searches $ initial_count $ min $ max)

let generic_test_spec ~count spec_args =
  let {min;max;initial_count;_} = spec_args in
  let all_elements = Util.gen_random_unique_array ~min ~max (initial_count + count) in
  (* Printf.printf "min: %d, max: %d\n" min max; *)
  let initial_elements = Array.sub all_elements 0 initial_count in
  (* let initial_elements = Util.gen_random_array ~min ~max initial_count in *)
  (* Array.iter (fun i -> Printf.printf "%d " i) (initial_elements ()); *)
  (* let insert_elements = Util.gen_random_array ~min ~max count in *)
  let insert_elements = Array.sub all_elements initial_count count in
  let search_elements = Util.gen_random_array ~min ~max spec_args.no_searches in
  if spec_args.sorted then
    Array.sort Int.compare insert_elements;
  { initial_elements; insert_elements; search_elements}

module Sequential = struct

  type t = IntYfastTrie.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec = 
    let initial_elements = test_spec.initial_elements in
    (* let size = Array.length initial_elements + Array.length test_spec.insert_elements in *)
    let skiplist = IntYfastTrie.init int_size in
    (* Printf.printf "initializing with %d elements\n" (Array.length initial_elements);
    Printf.printf "max element: %d\n" (Array.fold_left max 0 initial_elements); *)
    Array.iter (fun i -> IntYfastTrie.insert skiplist i) initial_elements;
    (* Printf.printf "initialized with %d elements\n" (Array.length initial_elements); *)
    skiplist

  let run _pool t test_spec =
    (* Printf.printf "inserting %d elements\n" (Array.length test_spec.insert_elements); *)
    Array.iter (fun i ->
        IntYfastTrie.insert t i) test_spec.insert_elements;
    (* Printf.printf "inserted %d elements\n" (Array.length test_spec.insert_elements);
    Printf.printf "searching %d elements\n" (Array.length test_spec.search_elements); *)
    Array.iter (fun i ->
        IntYfastTrie.mem t i |> ignore) test_spec.search_elements
    (* for _ = 1 to test_spec.size do
      IntYfastTrie.Sequential.size t |> ignore
    done *)

  let cleanup (_t: t) (test_spec: test_spec) = ()
    (* let all_elements = Array.concat [test_spec.insert_elements; test_spec.initial_elements] in
    Array.sort Int.compare all_elements;
    let all_elements = Array.to_list all_elements in
    let vebtree_flattened = IntYfastTrie.flatten _t in
    assert (all_elements = vebtree_flattened) *)

end


module CoarseGrained = struct

  type t = {skiplist : IntYfastTrie.t; mutex : Mutex.t}

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args


  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec = 
    let initial_elements = test_spec.initial_elements in
    (* let size = Array.length initial_elements + Array.length test_spec.insert_elements in *)
    let skiplist = IntYfastTrie.init int_size in
    Array.iter (fun i -> IntYfastTrie.insert skiplist i) initial_elements;
    {skiplist; mutex=Mutex.create ()}

  let run pool t test_spec =
    let total = Array.length test_spec.insert_elements + Array.length test_spec.search_elements - 1 in
    Domainslib.Task.parallel_for pool
      ~start:0 ~finish:total ~chunk_size:1
      ~body:(fun i ->
          Mutex.lock t.mutex;
          Fun.protect ~finally:(fun () -> Mutex.unlock t.mutex) (fun () ->
              if i < Array.length test_spec.insert_elements
              then IntYfastTrie.insert t.skiplist test_spec.insert_elements.(i)
              else if i < Array.length test_spec.insert_elements + Array.length test_spec.search_elements then
                ignore (IntYfastTrie.mem t.skiplist
                          test_spec.search_elements.(i - Array.length test_spec.insert_elements))
              (* else
                ignore (IntYfastTrie.Sequential.size t.skiplist) *)
            )
        )

  let cleanup (t: t) (test_spec: test_spec) = 
    let all_elements = Array.concat [test_spec.insert_elements; test_spec.initial_elements] in
    Array.sort Int.compare all_elements;
    let all_elements = Array.to_list all_elements in
    let vebtree_flattened = IntYfastTrie.flatten t.skiplist in
    assert (all_elements = vebtree_flattened)

end

module Batched = struct

  type t = BatchedYfastTrie.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init pool test_spec = 
    let initial_elements = test_spec.initial_elements in
    (* Printf.printf "initializing with %d elements\n" (Array.length initial_elements);
    Printf.printf "max element: %d\n" (Array.fold_left max 0 initial_elements); *)
    IntYfastTrieFunctor.universe_size := int_size;
    let skiplist = BatchedYfastTrie.init pool in
    Array.iter (fun i -> BatchedYfastTrie.apply skiplist (Insert i)) initial_elements;
    skiplist

  let run pool t test_spec =
    (* Printf.printf "inserting %d elements\n" (Array.length test_spec.insert_elements); *)
    let total = Array.length test_spec.insert_elements + Array.length test_spec.search_elements - 1 in
    Domainslib.Task.parallel_for pool
      ~start:0 ~finish:total ~chunk_size:1
      ~body:(fun i ->
          if i < Array.length test_spec.insert_elements
          then BatchedYfastTrie.apply t (Insert test_spec.insert_elements.(i))
          else if i < Array.length test_spec.insert_elements + Array.length test_spec.search_elements then
            ignore (BatchedYfastTrie.apply t 
                      (Member test_spec.search_elements.(i - Array.length test_spec.insert_elements)))
          (* else
            ignore (BatchedYfastTrie.apply t Size) *)
        );
    BatchedYfastTrie.wait_for_batch t

  let cleanup (t: t) (test_spec: test_spec) =
    (* Printf.printf "Checking now\n"; *)
    let t = BatchedYfastTrie.unsafe_get_internal_data t in
    let all_elements = Array.concat [test_spec.insert_elements; test_spec.initial_elements] in
    Array.sort Int.compare all_elements;
    let all_elements = Array.to_list all_elements in
    let vebtree_flattened = IntYfastTrie.flatten t in
    assert (all_elements = vebtree_flattened)

end

(* module ExplicitBatched = struct

  type t = ExplicitlyBatchedSkiplist.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec = 
    let initial_elements = test_spec.initial_elements () in
    let skiplist = ExplicitlyBatchedSkiplist.init () in
    Array.iter (fun i -> ExplicitlyBatchedSkiplist.Sequential.insert skiplist i) initial_elements;
    skiplist

  let run pool t test_spec =
    ExplicitlyBatchedSkiplist.par_insert t pool test_spec.insert_elements;
    if test_spec.size > 0 then ignore @@ ExplicitlyBatchedSkiplist.par_size t pool (Array.make test_spec.size 0);
    ignore @@ ExplicitlyBatchedSkiplist.par_search t pool test_spec.search_elements

  let cleanup (_t: t) (_test_spec: test_spec) = ()

end *)
