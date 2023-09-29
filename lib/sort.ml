module T = Domainslib.Task

let bubble_sort_threshold = 32

let bubble_sort ~compare (a : 'a array) start limit =
  for i = start to limit - 2 do
    for j = i + 1 to limit - 1 do
      if compare a.(j) a.(i)  < 0 then
        let t = a.(i) in
        a.(i) <- a.(j);
        a.(j) <- t;
    done
  done

let merge ~compare (src : 'a array) dst start split limit =
  let rec loop dst_pos i j =
    if i = split then
      Array.blit src j dst dst_pos (limit - j)
    else if j = limit then
      Array.blit src i dst dst_pos (split - i)
    else if compare src.(i) src.(j) <= 0 then begin
      dst.(dst_pos) <- src.(i);
      loop (dst_pos + 1) (i + 1) j;
    end else begin
      dst.(dst_pos) <- src.(j);
      loop (dst_pos + 1) i (j + 1);
    end in
  loop start start split

let rec merge_sort pool ~compare move a b start limit =
  if move || limit - start > bubble_sort_threshold then
    let split = (start + limit) / 2 in
    let r1 = T.async pool (fun () -> merge_sort pool ~compare (not move) a b start split) in
    let r2 = T.async pool (fun () -> merge_sort pool ~compare (not move) a b split limit) in
    T.await pool r1;
    T.await pool r2;
    if move then merge ~compare a b start split limit else merge ~compare b a start split limit
  else bubble_sort ~compare a start limit

let sort pool ~compare a =
  let b = Array.copy a in
  merge_sort pool ~compare false a b 0 (Array.length a)