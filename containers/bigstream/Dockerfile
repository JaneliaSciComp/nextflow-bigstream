# Create final image
FROM janeliascicomp/dask:2023.3.2

RUN dnf install -y \
        git \
        mesa-libGL

WORKDIR /app/clusterlib

# install cluster decorator
RUN git clone https://github.com/GFleishman/ClusterWrap.git .

WORKDIR /app/bigstream

# install bigstream
ENV BIGSTREAM_TAG=main
RUN git clone --branch $BIGSTREAM_TAG --depth 1 https://github.com/JaneliaSciComp/bigstream.git .

COPY env.yaml .
# Remove the data and the git repo to reduce space
RUN echo "Remove test data from the container" && rm -rf resources && rm -rf .git

# Use the base environment from the baseImage
RUN mamba env update -n base -f env.yaml

RUN cd /app/clusterlib \
 && pip install . \
 && cd /app/bigstream \
 && pip install .
