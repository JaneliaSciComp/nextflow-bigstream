# Create final image
FROM janeliascicomp/dask:2022.12.1

RUN dnf install -y \
        git \
        mesa-libGL

WORKDIR /app/clusterlib

# install cluster decorator
RUN git clone https://github.com/GFleishman/ClusterWrap.git .

WORKDIR /app/bigstream

COPY env.yaml .
COPY setup.py .
COPY scripts scripts
COPY bigstream bigstream

# Use the base environment from the baseImage
RUN mamba env update -n base -f env.yaml

RUN cd /app/clusterlib \
 && pip install . \
 && cd /app/bigstream \
 && pip install .
