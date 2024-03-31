module IntSkiplist = Obatcher_ds.Skiplist.Make(Int)
module BatchedSkiplist = Domainslib.Batcher.Make(Obatcher_ds.Skiplist.Make(Int))
module ExplicitlyBatchedSkiplist = Obatcher_ds.Skiplist.Make(Int)
module LazySkiplist = Obatcher_ds.Lazy_slist.Make(struct include Int let hash t = t end)

type generic_test_spec = {
  initial_elements: (unit -> int array);
  insert_elements : int array;
  search_elements : int array;
  size : int
}

type generic_spec_args = {
  sorted : bool;
  no_searches : int;
  no_size : int;
  initial_count: int;
  min: int;
  max: int;
}

let generic_spec_args: generic_spec_args Cmdliner.Term.t =
  let open Cmdliner in
  let sorted = Arg.(value @@ flag  @@ info ~doc:"whether the inserts should be sorted" ["s"; "sorted"]) in
  let no_searches =
    Arg.(value @@ opt (some int) None @@ info ~doc:"number of searches" ~docv:"NO_SEARCHES" ["n"; "no-searches"]) in
  let no_size = 
    Arg.(value @@ opt (some int) None @@ info ~doc:"number of size operation calls" ~docv:"NO_SIZE" ["n_sz"; "no-size"]) in
  let initial_count =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Initial number of operations" ["init-count"]) in
  let min =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Minimum value of data for random inputs" ["min"]) in
  let max =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Maximum value of data for random inputs" ["max"]) in
  Term.(const (fun sorted no_searches no_size initial_count min max ->
    {
      sorted;
      no_searches=Option.value ~default:0 no_searches;
      no_size=Option.value ~default:0 no_size;
      initial_count=Option.value ~default:0 initial_count;
      min=Option.value ~default:0 min;
      max=Option.value ~default:((Int.shift_left 1 30) - 1) max;
    }) $ sorted $ no_searches $ no_size $ initial_count $ min $ max)

let generic_test_spec ~count spec_args =
  let {min;max;initial_count;_} = spec_args in
  let initial_elements () = Util.gen_random_array ~min ~max initial_count in
  let insert_elements = Util.gen_random_array ~min ~max count in
  let search_elements = Util.gen_random_array ~min ~max spec_args.no_searches in
  if spec_args.sorted then
    Array.sort Int.compare insert_elements;
  { initial_elements; insert_elements; search_elements; size=spec_args.no_size }

module Sequential = struct

  type t = IntSkiplist.Sequential.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec = 
    let initial_elements = test_spec.initial_elements () in
    let size = Array.length initial_elements + Array.length test_spec.insert_elements in
    let skiplist = IntSkiplist.Sequential.init ~size () in
    Array.iter (fun i -> IntSkiplist.Sequential.insert skiplist i) initial_elements;
    skiplist

  let run _pool t test_spec =
    Array.iter (fun i ->
        IntSkiplist.Sequential.insert t i) test_spec.insert_elements;
    Array.iter (fun i ->
        IntSkiplist.Sequential.mem t i |> ignore) test_spec.search_elements;
    for _ = 1 to test_spec.size do
      IntSkiplist.Sequential.size t |> ignore
    done

  let cleanup (_t: t) (_test_spec: test_spec) = ()

end


module CoarseGrained = struct

  type t = {skiplist : IntSkiplist.Sequential.t; mutex : Mutex.t}

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args


  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec = 
    let initial_elements = test_spec.initial_elements () in
    let size = Array.length initial_elements + Array.length test_spec.insert_elements in
    let skiplist = IntSkiplist.Sequential.init ~size () in
    Array.iter (fun i -> IntSkiplist.Sequential.insert skiplist i) initial_elements;
    {skiplist; mutex=Mutex.create ()}

  let run pool t test_spec =
    let total = Array.length test_spec.insert_elements + Array.length test_spec.search_elements + test_spec.size - 1 in
    Domainslib.Task.parallel_for pool
      ~start:0 ~finish:total ~chunk_size:1
      ~body:(fun i ->
          Mutex.lock t.mutex;
          Fun.protect ~finally:(fun () -> Mutex.unlock t.mutex) (fun () ->
              if i < Array.length test_spec.insert_elements
              then IntSkiplist.Sequential.insert t.skiplist test_spec.insert_elements.(i)
              else if i < Array.length test_spec.insert_elements + Array.length test_spec.search_elements then
                ignore (IntSkiplist.Sequential.mem t.skiplist
                          test_spec.search_elements.(i - Array.length test_spec.insert_elements))
              else
                ignore (IntSkiplist.Sequential.size t.skiplist)
            )
        )

  let cleanup (_t: t) (_test_spec: test_spec) = ()

end

module Batched = struct

  type t = BatchedSkiplist.t

  type test_spec = generic_test_spec

  type spec_args = generic_spec_args

  let spec_args: spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init pool test_spec = 
    let initial_elements = test_spec.initial_elements () in
    let skiplist = BatchedSkiplist.init pool in
    let exposed_s = BatchedSkiplist.unsafe_get_internal_data skiplist in
    Array.iter (fun i -> IntSkiplist.Sequential.insert exposed_s i)
      initial_elements;
    BatchedSkiplist.restart_batcher_timer skiplist;
    skiplist

  let run pool t test_spec =
    let total = Array.length test_spec.insert_elements + Array.length test_spec.search_elements + test_spec.size - 1 in
    Domainslib.Task.parallel_for pool
      ~start:0 ~finish:total ~chunk_size:1
      ~body:(fun i ->
          if i < Array.length test_spec.insert_elements
          then BatchedSkiplist.apply t (Insert test_spec.insert_elements.(i))
          else if i < Array.length test_spec.insert_elements + Array.length test_spec.search_elements then
            ignore (BatchedSkiplist.apply t 
                      (Member test_spec.search_elements.(i - Array.length test_spec.insert_elements)))
          else
            ignore (BatchedSkiplist.apply t Size)
        );
    BatchedSkiplist.wait_for_batch t

  let cleanup (_t: t) (_test_spec: test_spec) = ()

end

module ExplicitBatched = struct

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

end


module Lazy = struct
  
  type t = LazySkiplist.Node.t
             
  type test_spec = generic_test_spec
    
  type spec_args = generic_spec_args
    
  let spec_args : spec_args Cmdliner.Term.t = generic_spec_args

  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init _pool test_spec =
    let initial_elements = test_spec.initial_elements () in
    LazySkiplist.init ();
    Array.iter (fun i -> ignore @@ LazySkiplist.add i) initial_elements;
    LazySkiplist.Node.Null
      
  let run pool _t test_spec =
    let total = Array.length test_spec.insert_elements + Array.length test_spec.search_elements + test_spec.size - 1 in
    Domainslib.Task.parallel_for pool
      ~start:0 ~finish:total ~chunk_size:1
      ~body:(fun i ->
          if i < Array.length test_spec.insert_elements
          then ignore @@ LazySkiplist.add test_spec.insert_elements.(i)
          else if i < Array.length test_spec.insert_elements + Array.length test_spec.search_elements then
            ignore (LazySkiplist.contains test_spec.search_elements.(i - Array.length test_spec.insert_elements))
          else
            ignore (LazySkiplist.size ())
        )

  let cleanup (_t: t) (_test_spec: test_spec) = ()
end

