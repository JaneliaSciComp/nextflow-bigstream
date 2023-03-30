PLATFORM="--platform=linux/x86_64"

docker build \
       -t registry.int.janelia.org/multifish/bigstream-dask:1.0 \
       ${PLATFORM} \
       containers/bigstream
