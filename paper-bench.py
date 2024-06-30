#!/usr/bin/env python3

# Prerequisites
import pandas as pd
import os
import utils

# domains = list(range(1, 17))
domains = list(range(1, 9))
no_iters = 2
build_results = utils.build_results_seq_opt

# Ensure results directory exists
os.makedirs('results', exist_ok=True)

# ## Split-join functor

# ### AVL tree

print('running avl_insert_only benchmarks')
avl_insert_only_results = build_results(["avltree-batched", "avltree-coarse-grained"], {
    'count': 1_000_000,
    'init_count': 2_000_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="avltree-sequential", values=domains)
df = pd.DataFrame.from_records(avl_insert_only_results, index="domains")
df.to_csv("results/avl_insert_only_results.csv")


# In[ ]:


print('running avl_search_only benchmarks')
avl_search_only_results = build_results(["avltree-batched", "avltree-coarse-grained"], {
    'count': 0,
    'init_count': 2_000_000,
    'no_searches': 1_000_000,
    'no_iters': no_iters
    }, sequential="avltree-sequential", values=domains)

df.to_csv("results/avl_search_only_results.csv")


# In[ ]:


print('running avl_90_10 benchmarks')
avl_90_10_results = build_results(["avltree-batched", "avltree-coarse-grained"], {
    'count': 100_000,
    'init_count': 2_000_000,
    'no_searches': 900_000,
    'no_iters': no_iters
    }, sequential="avltree-sequential", values=domains)
df = pd.DataFrame.from_records(avl_90_10_results, index="domains")
df.to_csv("results/avl_90_10_results.csv")

# In[ ]:
print('running avl_50_50 benchmarks')
avl_50_50_results = build_results(["avltree-batched", "avltree-coarse-grained"], {
    'count': 500_000,
    'init_count': 2_000_000,
    'no_searches': 500_000,
    'no_iters': no_iters
    }, sequential="avltree-sequential", values=domains)
df = pd.DataFrame.from_records(avl_50_50_results, index="domains")
df.to_csv("results/avl_50_50_results.csv")


# ### Red-Black tree


print('running rb_insert_only benchmarks')
rb_insert_only_results = build_results(["rbtree-batched", "rbtree-coarse-grained"], {
    'count': 1_000_000,
    'init_count': 2_000_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="rbtree-sequential", values=domains)
df = pd.DataFrame.from_records(rb_insert_only_results, index="domains")
df.to_csv("results/rb_insert_only_results.csv")


print('running rb_search_only benchmarks')
rb_search_only_results = build_results(["rbtree-batched", "rbtree-coarse-grained"], {
    'count': 0,
    'init_count': 2_000_000,
    'no_searches': 1_000_000,
    'no_iters': no_iters
    }, sequential="rbtree-sequential", values=domains)
df = pd.DataFrame.from_records(rb_search_only_results, index="domains")
df.to_csv("results/rb_search_only_results.csv")


# In[ ]:
print('running rb_90_10 benchmarks')
rb_90_10_results = build_results(["rbtree-batched", "rbtree-coarse-grained"], {
    'count': 100_000,
    'init_count': 2_000_000,
    'no_searches': 900_000,
    'no_iters': no_iters
    }, sequential="rbtree-sequential", values=domains)

df = pd.DataFrame.from_records(rb_90_10_results, index="domains")
df.to_csv("results/rb_90_10_results.csv")


# In[ ]:


print('running rb_50_50 benchmarks')
rb_50_50_results = build_results(["rbtree-batched", "rbtree-coarse-grained"], {
    'count': 500_000,
    'init_count': 2_000_000,
    'no_searches': 500_000,
    'no_iters': no_iters
    }, sequential="rbtree-sequential", values=domains)
df = pd.DataFrame.from_records(rb_50_50_results, index="domains")
df.to_csv("results/rb_50_50_results.csv")


# ### Treap

# In[ ]:


print('running treap_insert_only benchmarks')
treap_insert_only_results = build_results(["treap-batched", "treap-coarse-grained"], {
    'count': 1_000_000,
    'init_count': 2_000_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="treap-sequential", values=domains)
df = pd.DataFrame.from_records(treap_insert_only_results, index="domains")
df.to_csv("results/treap_insert_only_results.csv")


# In[ ]:


print('running treap_search_only benchmarks')
treap_search_only_results = build_results(["treap-batched", "treap-coarse-grained"], {
    'count': 0,
    'init_count': 2_000_000,
    'no_searches': 1_000_000,
    'no_iters': no_iters
    }, sequential="treap-sequential", values=domains)
df = pd.DataFrame.from_records(treap_search_only_results, index="domains")
df.to_csv("results/treap_search_only_results.csv")


# In[ ]:


print('running treap_90_10 benchmarks')
treap_90_10_results = build_results(["treap-batched", "treap-coarse-grained"], {
    'count': 100_000,
    'init_count': 2_000_000,
    'no_searches': 900_000,
    'no_iters': no_iters
    }, sequential="treap-sequential", values=domains)
df = pd.DataFrame.from_records(treap_90_10_results, index="domains")
df.to_csv("results/treap_90_10_results.csv")


# In[ ]:


print('running treap_50_50 benchmarks')
treap_50_50_results = build_results(["treap-batched", "treap-coarse-grained"], {
    'count': 500_000,
    'init_count': 2_000_000,
    'no_searches': 500_000,
    'no_iters': no_iters
    }, sequential="treap-sequential", values=domains)
df = pd.DataFrame.from_records(treap_50_50_results, index="domains")
df.to_csv("results/treap_50_50_results.csv")


# ## Expose-repair functor

# In[ ]:


print('running veb_insert_only benchmarks')
veb_insert_only_results = build_results(["vebtree-batched", "vebtree-coarse-grained"], {
    'count': 1_000_000,
    'init_count': 2_000_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="vebtree-sequential", values=domains)
df = pd.DataFrame.from_records(veb_insert_only_results, index="domains")
df.to_csv("results/veb_insert_only_results.csv")


# In[ ]:


print('running veb_search_only benchmarks')
veb_search_only_results = build_results(["vebtree-batched", "vebtree-coarse-grained"], {
    'count': 0,
    'init_count': 2_000_000,
    'no_searches': 1_000_000,
    'no_iters': no_iters
    }, sequential="vebtree-sequential", values=domains)
df = pd.DataFrame.from_records(veb_search_only_results, index="domains")
df.to_csv("results/veb_search_only_results.csv")


# In[ ]:


print('running veb_90_10 benchmarks')
veb_90_10_results = build_results(["vebtree-batched", "vebtree-coarse-grained"], {
    'count': 100_000,
    'init_count': 2_000_000,
    'no_searches': 900_000,
    'no_iters': no_iters
    }, sequential="vebtree-sequential", values=domains)
df = pd.DataFrame.from_records(veb_90_10_results, index="domains")
df.to_csv("results/veb_90_10_results.csv")


# In[ ]:


print('running veb_50_50 benchmarks')
veb_50_50_results = build_results(["vebtree-batched", "vebtree-coarse-grained"], {
    'count': 500_000,
    'init_count': 500_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="vebtree-sequential", values=domains)
df = pd.DataFrame.from_records(veb_50_50_results, index="domains")
df.to_csv("results/veb_50_50_results.csv")


# ## Xfast trie

# In[ ]:


print('running xfast_insert_only benchmarks')
xfast_insert_only_results = build_results(["xfast-batched", "xfast-coarse-grained"], {
    'count': 1_000_000,
    'init_count': 2_000_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="xfast-sequential", values=domains)
df = pd.DataFrame.from_records(xfast_insert_only_results, index="domains")
df.to_csv("results/xfast_insert_only_results.csv")


# In[ ]:


print('running xfast_search_only benchmarks')
xfast_search_only_results = build_results(["xfast-batched", "xfast-coarse-grained"], {
    'count': 0,
    'init_count': 2_000_000,
    'no_searches': 1_000_000,
    'no_iters': no_iters
    }, sequential="xfast-sequential", values=domains)
df = pd.DataFrame.from_records(xfast_search_only_results, index="domains")
df.to_csv("results/xfast_search_only_results.csv")


# In[ ]:


print('running xfast_90_10 benchmarks')
xfast_90_10_results = build_results(["xfast-batched", "xfast-coarse-grained"], {
    'count': 100_000,
    'init_count': 2_000_000,
    'no_searches': 900_000,
    'no_iters': no_iters
    }, sequential="xfast-sequential", values=domains)
df = pd.DataFrame.from_records(xfast_90_10_results, index="domains")
df.to_csv("results/xfast_90_10_results.csv")


# In[ ]:


print('running xfast_50_50 benchmarks')
xfast_50_50_results = build_results(["xfast-batched", "xfast-coarse-grained"], {
    'count': 500_000,
    'init_count': 2_000_000,
    'no_searches': 500_000,
    'no_iters': no_iters
    }, sequential="xfast-sequential", values=domains)
df = pd.DataFrame.from_records(xfast_50_50_results, index="domains")
df.to_csv("results/xfast_50_50_results.csv")


# ### Yfast trie

# In[ ]:


print('running yfast_insert_only benchmarks')
yfast_insert_only_results = build_results(["yfast-batched", "yfast-coarse-grained"], {
    'count': 1_000_000,
    'init_count': 2_000_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="yfast-sequential", values=domains)
df = pd.DataFrame.from_records(yfast_insert_only_results, index="domains")
df.to_csv("results/yfast_insert_only_results.csv")


# In[ ]:


print('running yfast_search_only benchmarks')
yfast_search_only_results = build_results(["yfast-batched", "yfast-coarse-grained"], {
    'count': 0,
    'init_count': 2_000_000,
    'no_searches': 1_000_000,
    'no_iters': no_iters
    }, sequential="yfast-sequential", values=domains)

df = pd.DataFrame.from_records(yfast_search_only_results, index="domains")
df.to_csv("results/yfast_search_only_results.csv")


# In[ ]:


print('running yfast_90_10 benchmarks')
yfast_90_10_results = build_results(["yfast-batched", "yfast-coarse-grained"], {
    'count': 100_000,
    'init_count': 2_000_000,
    'no_searches': 900_000,
    'no_iters': no_iters
    }, sequential="yfast-sequential", values=domains)
df = pd.DataFrame.from_records(yfast_90_10_results, index="domains")
df.to_csv("results/yfast_90_10_results.csv")


# In[ ]:


print('running yfast_50_50 benchmarks')
yfast_50_50_results = build_results(["yfast-batched", "yfast-coarse-grained"], {
    'count': 500_000,
    'init_count': 2_000_000,
    'no_searches': 500_000,
    'no_iters': no_iters
    }, sequential="yfast-sequential", values=domains)
df = pd.DataFrame.from_records(yfast_50_50_results, index="domains")
df.to_csv("results/yfast_50_50_results.csv")


# ## Ad-hoc data structures

# ### Skiplist

# In[ ]:


print('running skiplist_insert_only benchmarks')
skiplist_insert_only_results = build_results(["skiplist-batched", "skiplist-fine-grained","skiplist-coarse-grained"], {
    'count': 1_000_000,
    'init_count': 2_000_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="skiplist-sequential", values=domains)
df = pd.DataFrame.from_records(skiplist_insert_only_results, index="domains")
df.to_csv("results/skiplist_insert_only_results.csv")


# In[ ]:


print('running skiplist_search_only benchmarks')
skiplist_search_only_results = build_results(["skiplist-batched", "skiplist-fine-grained","skiplist-coarse-grained"], {
    'count': 0,
    'init_count': 2_000_000,
    'no_searches': 1_000_000,
    'no_iters': no_iters
    }, sequential="skiplist-sequential", values=domains)
df = pd.DataFrame.from_records(skiplist_search_only_results, index="domains")
df.to_csv("results/skiplist_search_only_results.csv")


# In[ ]:


print('running skiplist_50_50 benchmarks')
skiplist_50_50_results = build_results(["skiplist-batched","skiplist-fine-grained", "skiplist-coarse-grained"], {
    'count': 500_000,
    'init_count': 2_000_000,
    'no_searches': 500_000,
    'no_iters': no_iters
    }, sequential="skiplist-sequential", values=domains)
df = pd.DataFrame.from_records(skiplist_50_50_results, index="domains")
df.to_csv("results/skiplist_50_50_results.csv")


# In[ ]:


print('running skiplist_90_10 benchmarks')
skiplist_90_10_results = build_results(["skiplist-batched", "skiplist-fine-grained","skiplist-coarse-grained"], {
    'count': 100_000,
    'init_count': 2_000_000,
    'no_searches': 900_000,
    'no_iters': no_iters
    }, sequential="skiplist-sequential", values=domains)
df = pd.DataFrame.from_records(skiplist_90_10_results, index="domains")
df.to_csv("results/skiplist_90_10_results.csv")


# ### B-tree

# In[ ]:


print('running btree_insert_only benchmarks')
btree_insert_only_results = build_results(["btree-batched", "btree-coarse-grained"], {
    'count': 1_000_000,
    'init_count': 2_000_000,
    'no_searches': 0,
    'no_iters': no_iters
    }, sequential="btree-sequential", values=domains)
df = pd.DataFrame.from_records(btree_insert_only_results, index="domains")
df.to_csv("results/btree_insert_only_results.csv")


# In[ ]:


print('running btree_search_only benchmarks')
btree_search_only_results = build_results(["btree-batched", "btree-coarse-grained"], {
    'count': 0,
    'init_count': 2_000_000,
    'no_searches': 1_000_000,
    'no_iters': no_iters
    }, sequential="btree-sequential", values=domains)
df = pd.DataFrame.from_records(btree_search_only_results, index="domains")
df.to_csv("results/btree_search_only_results.csv")


# In[ ]:


print('running btree_50_50 benchmarks')
btree_50_50_results = build_results(["btree-batched", "btree-coarse-grained"], {
    'count': 500_000,
    'init_count': 2_000_000,
    'no_searches': 500_000,
    'no_iters': no_iters
    }, sequential="btree-sequential", values=domains)
df = pd.DataFrame.from_records(btree_50_50_results, index="domains")
df.to_csv("results/btree_50_50_results.csv")


# In[ ]:


print('running btree_90_10 benchmarks')
btree_90_10_results = build_results(["btree-batched", "btree-coarse-grained"], {
    'count': 100_000,
    'init_count': 2_000_000,
    'no_searches': 900_000,
    'no_iters': no_iters
    }, sequential="btree-sequential", values=domains)
df = pd.DataFrame.from_records(btree_90_10_results, index="domains")
df.to_csv("results/btree_90_10_results.csv")


# ### Datalog

# In[ ]:

print('running datalog benchmarks')
datalog_results = build_results(["datalog-batched", "datalog-coarse", "datalog-non-parallel-batched"], {
    'count': 5_000,
    'init_count': 30_000,
    'no_searches': 45_000,
    'graph_nodes': 200,
    'no_iters': no_iters
    }, sequential="datalog-sequential", values=domains)

df = pd.DataFrame.from_records(datalog_results, index="domains")
df.to_csv("results/datalog_results.csv")

