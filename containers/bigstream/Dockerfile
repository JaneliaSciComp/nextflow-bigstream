# Create final image
FROM janeliascicomp/dask:2023.3.2

RUN dnf install -y \
        git \
        mesa-libGL

WORKDIR /app/clusterlib

# install cluster decorator
RUN git clone https://github.com/GFleishman/ClusterWrap.git .

WORKDIR /app/bigstream

COPY env.yaml .
COPY setup.py .

# Use the base environment from the baseImage
RUN mamba env update -n base -f env.yaml

COPY scripts scripts
COPY bigstream bigstream

RUN cd /app/clusterlib \
 && pip install . \
 && cd /app/bigstream \
 && pip install .
