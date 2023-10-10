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

avltree-batched :
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

RB_PRESET := 2_000_000
RB_OPS := 1_000_000
RB_SEARCH := 0
rbtree : rbtree.ml
	printf "rbtree-Sequential\n"
	$(B) rbtree-sequential --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP)

	printf "rbtree-Batched\n"
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

rbtree-batched :
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

rbtree-validate :
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T

TREAP_PRESET := 2_000_000
TREAP_OPS := 1_000_000
TREAP_SEARCH := 0
treap : treap.ml
	printf "treap-Sequential\n"
	$(B) treap-sequential --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP)

	printf "treap-Batched\n"
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

treap-batched :
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

treap-validate :
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T