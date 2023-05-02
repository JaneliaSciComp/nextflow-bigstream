PLATFORM="--platform=linux/x86_64"

helpmsg="$0
    build
    push
    pushTags <containers>
    help
"

RUNNER=

TAG=registry.int.janelia.org/multifish/bigstream-dask:1.0

while [[ $# > 0 ]]; do
    key="$1"
    shift # past the key
    case ${key} in
       build)
            COMMAND=build
            TAG_ARG="-t $TAG"
            CONTAINERS_DIR_ARG="containers/bigstream"
            ;;
       push)
            COMMAND=push
            TAG_ARG=$TAG
            ;;
       --platform)
            PLATFORM=$1
            shift
            PLATFORM_ARG="--platform=${PLATFORM}"
            ;;
       -n)
            RUNNER=echo
            ;;
       *)
            echo ${helpmsg}
            exit 1
            ;;
    esac
done


$RUNNER docker ${COMMAND} \
       ${PLATFORM_ARG} \
       ${TAG_ARG} \
       ${CONTAINERS_DIR_ARG}
