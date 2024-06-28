# Batch Parallel Data Structures with Obatcher: Artefact

This repository contains the associated artefact for the OOPSLA paper
#608 "Concurrent Data Structures Made Easy".

This document will describe how to a) reproduce all the results in the
paper and b) to build and extend the project as a user.

We provide two ways for users to run this artefact --- 1, as a
self-contained docker-file, and 2, as an local OCaml project, although
this will require having opam installed on your system.

The artefact also contains an example instantiation of our batching
framework in Rust (under the directory `obatcher_in_rust`), and
running this will require having the rust toolchain installed.

## Setup
All following instructions will assume that you are running from the
project root.

### Setting up Dockerfile

1. Build the docker file

```
$ docker buildx build -tobatcher .
 => [1/3] FROM docker.io/ocaml/opam:ubuntu-22.04-ocaml-5.0@sha256:6906035  0.0s
...
Successfully tagged obatcher:latest
```

This command will build a new docker image for obatcher, downloading
any required system dependencies and OCaml packages, and building
Sisyphus.

This process will take approximately 5 minutes on a commodity laptop.

Once the image has been built, you can launch a container using the image:

```
$ docker run --rm -it obatcher bash
opam@c19e1ac10c2d:~$ ls
Dockerfile  benchmark     lib               opam-repository
README.md   dune          obatcher          requirements.txt
_build      dune-project  obatcher_ds.opam  test
```

To make sure all subsequent commands run correctly, make sure to run the following before proceeding:

```
$ eval $(opam env)
```

The subsequent steps of this guide will assume that you are operating
inside this container and will show how to run Sisyphus and produce
the benchmark results.

### Local Install

1. Set up a new opam switch for obatcher:

```
opam switch create obatcher 5.0.0
eval $(opam env)
```

2. Install dependencies:

```
opam update
opam install dune batteries.3.8.0 progress.0.4.0 ptime.1.1.0 cmdliner.1.3.0 datalog.0.4.1
```

3. Pin our version of domainslib (as mentioned in S.3.1 in the paper, we modify the base domainslib library to expose a `Promise.create` primitive to allow embedding our batching construction):
```
opam pin add domainslib ./obatcher
```

4. Build the project:

```
$ dune build
File "_none_", line 1:                 
Warning 58 [no-cmx-file]: no cmx file was found in path for module Datalog, and its interface was not compiled with -opaque
```
Ignore the warning regarding the lack of a cmx file for Datalog, this is warning arising from the datalog library and not part of this development

This command should complete within less than a minute. 


## Kick the tires
Assuming the obatcher project has been built successfully, you can
quickly test whether the installation is working by running the
project's test suite:

```
$ dune runtest

Starting insertion test for Yfast trie...
Insertion time for Yfast trie: 8.122952s
Deletion time for Yfast trie: 3.417661s
Verification time for Yfast trie: 1.016395s

...
```

This should a few (~2) minutes. If it completes successfully, then the
artefact is running correctly and you will be able to reproduce our
tables.

Note: if you see that certain build tasks are failing because they are
KILL-ed and you are running in Docker:
```
Verification time for predecessor test: 1.236215s
File "test/dune", line 28, characters 7-21:    ^[v
28 |  (name test_rbfunctor)
            ^^^^^^^^^^^^^^
Command got signal KILL.
```

This is likely a sign that your docker engine is not allocating enough
memory to the container and as such tasks are being killed by OOM.



## Reproducing experiments
In the paper we ran our experiments on an AWS EC2 c7i.12xlarge server
instance equipped x86 Intel® Xeon® Scalable (Sapphire Rapids)
processor with 24 physical cores and 96 GB of memory, running Ubuntu
22.02 with OCaml 5.1.1. [1]

As this requires dedicated hardware to fully reproduce, our artefact
provides a scaled down set of experiments that should produce the same
trends while being viable to run on commodity hardware.

In order to reproduce the experiments, run the script
`paper-bench.py`. This will run all experiments from the paper, on up
to 8 cores[2] on your machine, and dump the timing information into
the results folder.

Note: The script runs 33 experiments in total, each experiment
considers a different configuration of data-structure and
inserts/searches split, and measures the overall time for varying
numbers of domains, from 1 to 8. Each such run takes ~5 minutes, so
overall, the time taken for the script to run will be `8 * 33 * 5` ~
1320 mins or around 22 hours.

Once the script has completed, you will be able to recreate Figure 23
and Figure 24 from the paper by running pdflatex on the two `.tex`
files in the results directory:

- Reproducing figure 23:
   ```
   $ (cd ./results && pdflatex search-plots.tex)
   $ open ./results/search-plots.pdf
   ```

- Reproducing figure 24:

   ```
   $ (cd ./results && pdflatex datalog-plot.tex)
   $ open ./results/datalog-plot.pdf
   ```

Note: if you are running inside a docker container, then you can retrieve the generated pdf files on your host by copying

```
$ docker cp <container-id>:/home/opam/results/search-plots.pdf ./
$ docker cp <container-id>:/home/opam/results/datalog-plot.pdf ./
```

You can retrieve the container id by running `docker container ls`:

```
$ docker container ls 
CONTAINER ID   IMAGE             COMMAND               CREATED          STATUS          PORTS     NAMES
1a7475605c74   obatcher:latest   "opam exec -- bash"   53 seconds ago   Up 51 seconds             elastic_moore
```


[1] Optionally, in order to reproduce the experiments on the hardware
used in the paper, if requested we can provide limited time access to
an AWS server to run the experiments --- to keep costs down, the
server can only be kept up for a couple of days, so please contact the
authors promptly if you wish to perform this evaluation.

[2] `paper-bench.py` only runs the experiments up to 8 domains, while
the paper shows trends up to 16. If running on a machine that has up
to 16 cores, then one can run experiments over more cores by changing
line 9 in `paper-bench.py` to `# domains = list(range(1, 17))`


# Batch Parallel Data Structures with Obatcher: Developer Instructions

The following instructions are supplied to help developers build and
construct new experiments on top of this development

As this requires a custom version of the library `domainslib` for
Multicore OCaml, it is advisable that you create a new `opam` switch
to run this.

## Build instructions

First, set up OBatcher along with a new `opam` switch:

```
opam switch create obatcher_test ocaml.5.0.0
eval $(opam env)
opam update
opam install dune batteries progress ptime cmdliner datalog
opam pin add domainslib https://github.com/phongulus/obatcher.git\#wait-for-batch
```

Note: for VSCode users, you will want to run the following as well to reinstall the prerequisites for the OCaml Platform extension in the new switch:

```
opam install ocamlformat ocaml-lsp-server
```

You can now clone and build the code in this repository.

```
git clone https://github.com/phongulus/obatcher_ds.git obatcher_ds
cd obatcher_ds
dune build
```

## Running tests and benchmarks

Provided tests can be simply run after building with:

```
dune runtest
```

For benchmarking, navigate to the `benchmark/` directory and use `make` commands:

```
cd benchmark
make avltree-batched
```

See the Makefile for the full list of benchmarks that can be run this way. A more advisable way to run the benchmarks would be to use the provided Jupyter Notebook. It is recommended to set up a Python virtual environment for this purpose:

```
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

When running the Jupyter Notebook, use the virtual environment set up above.


## Project structure 

The project is structured as follows:

```
.
|-- README.md
|-- obatcher_ds.opam
|-- requirements.txt
|-- dune
|-- dune-project
|-- lib/
|-- benchmark/
|-- test/
`-- obatcher/
```

The directories are:

- `lib/` -- contains the implementation of our functors and instantiations for each of the data-structures described in the paper
- `benchmark/` -- contains code for running the experiments from the paper
- `test/` -- unit tests to check the correctness of our implementations
- `obatcher/` -- lightly modified version of OCaml's domainslib library that exposes a `Promise.create` function.
