B := ../_build/default/benchmark/bench.exe
WARMUP := 5
ITER := 5

B_PRESET := 2_000_000
B_OPS := 1_000_000
B_SEARCH := 0
btree : btree.ml
	printf "Btree-Sequential\n"
	$(B) btree-sequential --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP)

	printf "Btree-Coarse-Grained\n"
	$(B) btree-coarse-grained --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) btree-coarse-grained --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) btree-coarse-grained --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) btree-coarse-grained --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) btree-coarse-grained --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) btree-coarse-grained --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) btree-coarse-grained --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) btree-coarse-grained --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

	printf "Btree-Batched\n"
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

btree-batched :
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 3 -T
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 4 -T
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 5 -T
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 6 -T
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 7 -T
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 8 -T

AVL_PRESET := 2_000_000
AVL_OPS := 1_000_000
AVL_SEARCH := 0
avltree : avltree.ml
	printf "Avltree-Sequential\n"
	$(B) avltree-sequential --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T

	printf "Avltree-Batched\n"
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 3 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 4 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 5 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 6 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 7 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 8 -T

avltree-batched :
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 3 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 4 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 5 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 6 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 7 -T
	$(B) avltree-batched --init-count=$(AVL_PRESET) --count=$(AVL_OPS) --no-searches=$(AVL_SEARCH) -i $(ITER) -w $(WARMUP) -D 8 -T

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
	$(B) treap-sequential --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -T

	printf "treap-Batched\n"
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 3 -T
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 4 -T
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 5 -T
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 6 -T
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 7 -T
	$(B) treap-batched --init-count=$(TREAP_PRESET) --count=$(TREAP_OPS) --no-searches=$(TREAP_SEARCH) -i $(ITER) -w $(WARMUP) -D 8 -T

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

LL_PRESET := 20_000
LL_OPS := 10_000
LL_SEARCH := 0
linkedlist : orderedlinkedlist.ml
	printf "Linkedlist-Sequential\n"
	$(B) linkedlist-sequential --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T

	printf "Linkedlist-Batched\n"
	$(B) linkedlist-batched --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T
	$(B) linkedlist-batched --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T
	$(B) linkedlist-batched --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 3 -T
	$(B) linkedlist-batched --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 4 -T
	$(B) linkedlist-batched --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 5 -T
	$(B) linkedlist-batched --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 6 -T
	$(B) linkedlist-batched --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 7 -T
	$(B) linkedlist-batched --init-count=$(LL_PRESET) --count=$(LL_OPS) --no-searches=$(LL_SEARCH) -i $(ITER) -w $(WARMUP) -D 8 -T