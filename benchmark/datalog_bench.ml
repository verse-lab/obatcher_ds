module Datalog = Datalog.Default

let with_graph_db f =
  let db = Datalog.db_create () in
  let connect_trans =
    let x = Datalog.mk_var 1 in
    let y = Datalog.mk_var 2 in
    let z = Datalog.mk_var 3 in
    Datalog.mk_clause (Datalog.mk_literal "connected" [x;z]) [
      Datalog.mk_literal "connected" [x;y];
      Datalog.mk_literal "connected" [y;z]
    ] in  
  let connect_edge = let x = Datalog.mk_var 1 and y = Datalog.mk_var 2 in
    Datalog.mk_clause (Datalog.mk_literal "connected" [x;y]) [Datalog.mk_literal "edge" [x;y]] in

  Datalog.db_add db connect_trans;
  Datalog.db_add db connect_edge;
  f db



module BatchedBasicDatalog = struct
  type t = Datalog.db
  type _ op =
    | Search : Datalog.literal -> bool op
    | Insert : Datalog.literal -> unit op

  type wrapped_op = Mk: 'a op * ('a -> unit) -> wrapped_op

  let init () =
    with_graph_db (fun db -> db)

  let run (t: t) (_pool: Domainslib.Task.pool) (batch: wrapped_op array) : unit =
    let searches : (Datalog.literal * (bool -> unit)) list ref = ref []
    and inserts : Datalog.literal list ref  = ref [] in
    Array.iter (function
        Mk (Search lit, kont) -> searches := (lit,kont) :: !searches
      | Mk (Insert clause, kont) -> kont (); inserts := (clause) :: !inserts
    ) batch;
    let searches = Array.of_list !searches in
    Array.iter  (fun (clause, kont) ->
      let res = ref false in
      Datalog.db_match t clause
        (fun _ -> res := true);
      kont !res
    ) searches;
    let inserts = Array.of_list !inserts in
    Array.iter (fun e ->
      Datalog.db_add_fact t e
    ) inserts

end


module BatchedDatalog = struct
  type t = Datalog.db
  type _ op =
    | Search : Datalog.literal -> bool op
    | Insert : Datalog.literal -> unit op

  type wrapped_op = Mk: 'a op * ('a -> unit) -> wrapped_op

  let init () =
    with_graph_db (fun db -> db)


  let run (t: t) (pool: Domainslib.Task.pool) (batch: wrapped_op array) : unit =
    let searches : (Datalog.literal * (bool -> unit)) list ref = ref []
    and inserts : Datalog.literal list ref  = ref [] in
    Array.iter (function
        Mk (Search lit, kont) -> searches := (lit,kont) :: !searches
      | Mk (Insert clause, kont) -> kont (); inserts := (clause) :: !inserts
    ) batch;
    let searches = Array.of_list !searches in
    Domainslib.Task.parallel_for pool ~start:0 ~finish:(Array.length searches - 1)
      ~body:(fun i ->
        let clause, kont = searches.(i) in
        let res = ref false in
        Datalog.db_match t clause
          (fun _ -> res := true);
        kont !res
      );
    let inserts = Array.of_list !inserts in
    Array.iter (fun e ->
      Datalog.db_add_fact t e
    ) inserts

end

module ConcurrentDatalog = Domainslib.Batcher.Make (BatchedDatalog)
module ConcurrentBasicDatalog = Domainslib.Batcher.Make (BatchedBasicDatalog)

type generic_spec_args = {
  no_searches: int;
  initial_count: int;
  graph_nodes: int;
  expensive_searches: bool;
}

type generic_test_spec = {
  args: generic_spec_args;
  count: int;
  mutable initial_elements: Datalog.literal array;
  mutable insert_elements: Datalog.literal array;
  mutable search_elements: Datalog.literal array;
}


let generic_spec_args: generic_spec_args Cmdliner.Term.t =
  let open Cmdliner in
  let no_searches =
    Arg.(required @@ opt (some int) None @@ info ~doc:"number of queries on the graph" ~docv:"NO_SEARCHES" ["n"; "no-searches"]) in
  let initial_count =
    Arg.(required @@ opt (some int) None @@ info ~doc:"Initial number of edges in the graph" ["init-count"]) in
  let graph_nodes =
    Arg.(value @@ opt (some int) None @@ info ~doc:"Number of nodes in the graph" ["graph-nodes"]) in
  let expensive_searches =
    Arg.(value @@ flag @@ info ~doc:"Whether searches should be expensive" ["expensive-searches"]) in

  Term.(const (fun no_searches initial_count graph_nodes expensive_searches ->
    let graph_nodes =
      Option.value graph_nodes
        ~default:((initial_count + no_searches) * 5) in
    {
      no_searches;
      initial_count;
      graph_nodes;
      expensive_searches
    }) $ no_searches $ initial_count $ graph_nodes $ expensive_searches)


let generic_test_spec ~count spec_args =
  {
    args=spec_args;
    count;
    initial_elements=[||];
    insert_elements=[||];
    search_elements=[||];
  }

let generic_init test_spec =
  let spec_args = test_spec.args in
  let node_to_symbol i  = "e" ^ string_of_int i in
  let node_to_term i  = Datalog.mk_const (node_to_symbol i) in
  let edge_to_literal (t1, t2)  = Datalog.mk_literal "edge" [node_to_term t1; node_to_term t2] in
  let edge_to_connected_literal (t1, t2)  =
    if test_spec.args.expensive_searches
    then Datalog.mk_literal "connected" [node_to_term t1; Datalog.mk_var 1]
    else Datalog.mk_literal "connected" [node_to_term t1; node_to_term t2] in
  let random_edge () =
    let i1 = Random.int (spec_args.graph_nodes + 1) in
    let i2 = Random.int (spec_args.graph_nodes + 1) in
    let edge = (i1, i2) in
    edge in
  if test_spec.args.graph_nodes < 1_000_000 then begin
    let n = test_spec.args.graph_nodes in
    let edges = Array.init (n * n) (fun i ->
      (Random.int (n * n), (i / n, i mod n))
    ) in
    Array.sort (fun (k, _) (k', _) -> Int.compare k k') edges;
    test_spec.initial_elements <-
      Array.init spec_args.initial_count
        (fun i -> edge_to_literal (snd edges.(i)));

    test_spec.insert_elements <-
      Array.init test_spec.count
        (fun i -> edge_to_literal (snd edges.(spec_args.initial_count + i)));
  end else begin
    let seen_edges :((int * int), unit) Hashtbl.t = Hashtbl.create (spec_args.graph_nodes * 100) in
    let rec fresh_edge () =
      let edge = random_edge () in
      if Hashtbl.mem seen_edges edge
      then fresh_edge ()
      else (Hashtbl.add seen_edges edge (); edge_to_literal edge) in

    test_spec.initial_elements <-
      Array.init spec_args.initial_count (fun _ -> fresh_edge ());

    test_spec.insert_elements <-
      Array.init test_spec.count (fun _ -> fresh_edge ());
  end;
  test_spec.search_elements <-
    Array.init spec_args.no_searches (fun _ -> edge_to_connected_literal (random_edge ()))


module Sequential = struct

  type t = Datalog.db

  type spec_args = generic_spec_args
  let spec_args = generic_spec_args

  type test_spec = {
    test_spec: generic_test_spec;
    mutable operations: [`Search of Datalog.literal | `Insert of Datalog.literal] array
  }

  let test_spec ~count spec_args =
    let test_spec = generic_test_spec ~count spec_args in
    {test_spec;operations=[||]}


  let init _pool spec =
    generic_init spec.test_spec;
    let total_elts = Array.length spec.test_spec.search_elements +
                     Array.length spec.test_spec.insert_elements in
    let conjoined_arrs =
      Array.append
        (Array.map (fun s -> `Search s) spec.test_spec.search_elements)
        (Array.map (fun s -> `Insert s) spec.test_spec.insert_elements) 
      |> Array.map (fun v -> Random.int total_elts, v) in
    Array.sort (fun (k, _) (k', _) -> Int.compare k k') conjoined_arrs;
    spec.operations <- Array.map snd conjoined_arrs;
    with_graph_db @@ fun db ->
    Array.iter (fun lit -> Datalog.db_add_fact db lit) spec.test_spec.initial_elements;
    db

  let run _pool (t: t) test_spec =
    for i = 0 to Array.length test_spec.operations - 1 do
      match test_spec.operations.(i) with
      | `Insert clause ->
        Datalog.db_add_fact t clause
      | `Search clause ->
        let res = ref false in
        Datalog.db_match t clause
          (fun _ -> res := true);
        ignore !res
    done

  let cleanup _ _ = ()

end


module CoarseGrained = struct

  type t = {
    db: Datalog.db;
    lock: Mutex.t
  }

  type test_spec = generic_test_spec
  type spec_args = generic_spec_args

  let spec_args = generic_spec_args
  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args


  let init _pool spec =
    generic_init spec;
    with_graph_db @@ fun db ->
    Array.iter (fun lit -> Datalog.db_add_fact db lit) spec.initial_elements;
    {db; lock=Mutex.create ()}


  let run pool (t: t) test_spec =
    Domainslib.Task.parallel_for pool ~chunk_size:1
      ~start:0 ~finish:(Array.length test_spec.insert_elements + Array.length test_spec.search_elements - 1)
      ~body:(fun i ->
        Mutex.lock t.lock;
        Fun.protect ~finally:(fun () -> Mutex.unlock t.lock) (fun () ->
          if i < Array.length test_spec.insert_elements
          then Datalog.db_add_fact t.db test_spec.insert_elements.(i)
          else ignore (
            let res = ref false in
            Datalog.db_match t.db test_spec.search_elements.(i - Array.length test_spec.insert_elements)
              (fun _ -> res := true);
            !res)
        )
      )

  let cleanup _ _ = ()

end

module BatchParallel = struct

  type t = ConcurrentDatalog.t

  type test_spec = generic_test_spec
  type spec_args = generic_spec_args

  let spec_args = generic_spec_args
  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init pool spec =
    generic_init spec;
    let t = ConcurrentDatalog.init pool in
    let exposed_t = ConcurrentDatalog.unsafe_get_internal_data t in
    Array.iter (fun lit ->
      Datalog.db_add_fact exposed_t lit
    ) spec.initial_elements;
    ConcurrentDatalog.restart_batcher_timer t;
    (* Array.iter (fun lit ->
      ConcurrentDatalog.apply t (BatchedDatalog.Insert lit)
    ) spec.initial_elements; *)
    t

  let run pool (t: t) test_spec =
    Domainslib.Task.parallel_for pool ~chunk_size:1
      ~start:0 ~finish:(Array.length test_spec.insert_elements + Array.length test_spec.search_elements - 1)
      ~body:(fun i ->
        if i < Array.length test_spec.insert_elements
        then ConcurrentDatalog.apply t (Insert test_spec.insert_elements.(i))
        else ignore (
          ignore @@
          ConcurrentDatalog.apply t (Search test_spec.search_elements.(i - Array.length test_spec.insert_elements)))
      );
    ConcurrentDatalog.wait_for_batch t

  let cleanup _ _ = ()

end

module BatchParallelBasic = struct

  type t = ConcurrentBasicDatalog.t

  type test_spec = generic_test_spec
  type spec_args = generic_spec_args

  let spec_args = generic_spec_args
  let test_spec ~count spec_args =
    generic_test_spec ~count spec_args

  let init pool spec =
    generic_init spec;
    let t = ConcurrentBasicDatalog.init pool in
    let exposed_t = ConcurrentBasicDatalog.unsafe_get_internal_data t in
    Array.iter (fun lit ->
      Datalog.db_add_fact exposed_t lit) spec.initial_elements;
    ConcurrentBasicDatalog.restart_batcher_timer t;
    (* Array.iter (fun lit ->
      ConcurrentBasicDatalog.apply t (BatchedBasicDatalog.Insert lit)
    ) spec.initial_elements; *)
    t

  let run pool (t: t) test_spec =
    Domainslib.Task.parallel_for pool ~chunk_size:1
      ~start:0 ~finish:(Array.length test_spec.insert_elements + Array.length test_spec.search_elements - 1)
      ~body:(fun i ->
        if i < Array.length test_spec.insert_elements
        then ConcurrentBasicDatalog.apply t (Insert test_spec.insert_elements.(i))
        else ignore (
          ignore @@
          ConcurrentBasicDatalog.apply t (Search test_spec.search_elements.(i - Array.length test_spec.insert_elements)))
      );
    ConcurrentBasicDatalog.wait_for_batch t

  let cleanup _ _ = ()

end



