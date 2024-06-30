FROM ocaml/opam:ubuntu-22.04-ocaml-5.0
ADD --chown=opam:opam . .
RUN opam update && \
    opam install dune batteries.3.8.0 progress.0.4.0 ptime.1.1.0 cmdliner.1.3.0 datalog.0.4.1 && \
    opam pin domainslib ./obatcher && \
    eval $(opam env) && dune build && \
    sudo apt install python3 python3-pip texlive texlive-pictures && \
    pip3 install tqdm pandas matplotlib
