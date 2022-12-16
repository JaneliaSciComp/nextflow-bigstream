# Create final image
FROM janeliascicomp/dask:2022.11.1

RUN dnf install -y git

WORKDIR /app/clusterlib

# install cluster decorator
RUN git clone https://github.com/GFleishman/ClusterWrap.git . && \
    pip install .

WORKDIR /app/bigstream

COPY env.yaml .
COPY setup.py .
COPY scripts scripts
COPY bigstream bigstream

# Use the base environment from the baseImage
RUN mamba env update -n base -f env.yaml \
 && pip install .
