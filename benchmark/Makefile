B := ../_build/default/benchmark/bench.exe
WARMUP := 5
ITER := 5

B_PRESET := 0
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
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	$(B) btree-batched --init-count=$(B_PRESET) --count=$(B_OPS) --no-searches=$(B_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

AVL_PRESET := 2_000_000
AVL_OPS := 0
AVL_SEARCH := 1_000_000
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
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 3 -T
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 4 -T
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 5 -T
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 6 -T
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 7 -T
	$(B) rbtree-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 8 -T

rbtree-explicitly-batched :
	$(B) rbtree-explicitly-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 1 -T
	$(B) rbtree-explicitly-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 2 -T
	$(B) rbtree-explicitly-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 3 -T
	$(B) rbtree-explicitly-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 4 -T
	$(B) rbtree-explicitly-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 5 -T
	$(B) rbtree-explicitly-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 6 -T
	$(B) rbtree-explicitly-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 7 -T
	$(B) rbtree-explicitly-batched --init-count=$(RB_PRESET) --count=$(RB_OPS) --no-searches=$(RB_SEARCH) -i $(ITER) -w $(WARMUP) -D 8 -T

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

VEB_PRESET := 2_000_000
VEB_INS := 1_000_000
# Need to fix assertion error with search
VEB_SEARCH := 0
vebtree : vebtree.ml
	printf "vebtree-Sequential\n"
	$(B) vebtree-sequential --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP)

	# printf "vebtree-Coarse-Grained\n"
	# $(B) vebtree-coarse-grained --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	# $(B) vebtree-coarse-grained --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	# $(B) vebtree-coarse-grained --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	# $(B) vebtree-coarse-grained --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	# $(B) vebtree-coarse-grained --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	# $(B) vebtree-coarse-grained --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	# $(B) vebtree-coarse-grained --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	# $(B) vebtree-coarse-grained --init-count=$(VEB_PRESET) --count=$(VEB_INS) --no-searches=$(VEB_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

	printf "vebtree-Batched\n"
	$(B) vebtree-batched --init-count=$(VEB_PRESET) --count=$(VEB_INS) -i $(ITER) --no-searches=$(VEB_SEARCH) -w $(WARMUP) -D 1
	$(B) vebtree-batched --init-count=$(VEB_PRESET) --count=$(VEB_INS) -i $(ITER) --no-searches=$(VEB_SEARCH) -w $(WARMUP) -D 2
	$(B) vebtree-batched --init-count=$(VEB_PRESET) --count=$(VEB_INS) -i $(ITER) --no-searches=$(VEB_SEARCH) -w $(WARMUP) -D 3
	$(B) vebtree-batched --init-count=$(VEB_PRESET) --count=$(VEB_INS) -i $(ITER) --no-searches=$(VEB_SEARCH) -w $(WARMUP) -D 4
	$(B) vebtree-batched --init-count=$(VEB_PRESET) --count=$(VEB_INS) -i $(ITER) --no-searches=$(VEB_SEARCH) -w $(WARMUP) -D 5
	$(B) vebtree-batched --init-count=$(VEB_PRESET) --count=$(VEB_INS) -i $(ITER) --no-searches=$(VEB_SEARCH) -w $(WARMUP) -D 6
	$(B) vebtree-batched --init-count=$(VEB_PRESET) --count=$(VEB_INS) -i $(ITER) --no-searches=$(VEB_SEARCH) -w $(WARMUP) -D 7
	$(B) vebtree-batched --init-count=$(VEB_PRESET) --count=$(VEB_INS) -i $(ITER) --no-searches=$(VEB_SEARCH) -w $(WARMUP) -D 8


XFAST_PRESET := 2_000_000
XFAST_INS := 1_000_000
# Need to fix assertion error with search
XFAST_SEARCH := 0
xfast : xfast.ml
	printf "xfast-Sequential\n"
	$(B) xfast-sequential --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP)

	# printf "xfast-Coarse-Grained\n"
	# $(B) xfast-coarse-grained --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP) -D 1
	# $(B) xfast-coarse-grained --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP) -D 2
	# $(B) xfast-coarse-grained --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP) -D 3
	# $(B) xfast-coarse-grained --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP) -D 4
	# $(B) xfast-coarse-grained --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP) -D 5
	# $(B) xfast-coarse-grained --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP) -D 6
	# $(B) xfast-coarse-grained --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP) -D 7
	# $(B) xfast-coarse-grained --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) --no-searches=$(XFAST_SEARCH) -i $(ITER) -w $(WARMUP) -D 8

	printf "xfast-Batched\n"
	$(B) xfast-batched --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) -i $(ITER) --no-searches=$(XFAST_SEARCH) -w $(WARMUP) -D 1
	$(B) xfast-batched --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) -i $(ITER) --no-searches=$(XFAST_SEARCH) -w $(WARMUP) -D 2
	$(B) xfast-batched --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) -i $(ITER) --no-searches=$(XFAST_SEARCH) -w $(WARMUP) -D 3
	$(B) xfast-batched --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) -i $(ITER) --no-searches=$(XFAST_SEARCH) -w $(WARMUP) -D 4
	$(B) xfast-batched --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) -i $(ITER) --no-searches=$(XFAST_SEARCH) -w $(WARMUP) -D 5
	$(B) xfast-batched --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) -i $(ITER) --no-searches=$(XFAST_SEARCH) -w $(WARMUP) -D 6
	$(B) xfast-batched --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) -i $(ITER) --no-searches=$(XFAST_SEARCH) -w $(WARMUP) -D 7
	$(B) xfast-batched --init-count=$(XFAST_PRESET) --count=$(XFAST_INS) -i $(ITER) --no-searches=$(XFAST_SEARCH) -w $(WARMUP) -D 8

YFAST_PRESET := 2_000_000
YFAST_INS := 1_000_000
# Need to fix assertion error with search
YFAST_SEARCH := 0
yfast : yfast.ml
	printf "yfast-Sequential\n"
	$(B) yfast-sequential --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) --no-searches=$(YFAST_SEARCH) -i $(ITER) -w $(WARMUP)

	printf "yfast-Batched\n"
	$(B) yfast-batched --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) -i $(ITER) --no-searches=$(YFAST_SEARCH) -w $(WARMUP) -D 1
	$(B) yfast-batched --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) -i $(ITER) --no-searches=$(YFAST_SEARCH) -w $(WARMUP) -D 2
	$(B) yfast-batched --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) -i $(ITER) --no-searches=$(YFAST_SEARCH) -w $(WARMUP) -D 3
	$(B) yfast-batched --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) -i $(ITER) --no-searches=$(YFAST_SEARCH) -w $(WARMUP) -D 4
	$(B) yfast-batched --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) -i $(ITER) --no-searches=$(YFAST_SEARCH) -w $(WARMUP) -D 5
	$(B) yfast-batched --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) -i $(ITER) --no-searches=$(YFAST_SEARCH) -w $(WARMUP) -D 6
	$(B) yfast-batched --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) -i $(ITER) --no-searches=$(YFAST_SEARCH) -w $(WARMUP) -D 7
	$(B) yfast-batched --init-count=$(YFAST_PRESET) --count=$(YFAST_INS) -i $(ITER) --no-searches=$(YFAST_SEARCH) -w $(WARMUP) -D 8