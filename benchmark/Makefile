B := ../_build/default/benchmark/bench.exe
WARMUP := 5
ITER := 5

AVL_PRESET := 2_000_000
AVL_OPS := 1_000_000
AVL_SEARCH := 0
avltree : avltree.ml
	printf "Avltree-Sequential\n"
	$(B) avltree-sequential --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP)

	printf "Avltree-Batched\n"
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

avltree_batched :
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

avltree-validate :
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T