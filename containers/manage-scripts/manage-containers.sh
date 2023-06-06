helpmsg="$0
    build|push|--help
"

RUNNER=

TAG=registry.int.janelia.org/multifish/bigstream-dask:1.0

additional_args=()

while [[ $# > 0 ]]; do
    key="$1"
    shift # past the key
    case ${key} in
       build)
           COMMAND="buildx build"
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
           PLATFORM_ARG="--platform ${PLATFORM}"
           ;;
       -n)
           RUNNER=echo
           ;;
       --help|-h)
           echo ${helpmsg}
           exit 1
           ;;
       *)
           additional_args=(${additional_args[@]} $key)
           ;;
    esac
done

$RUNNER docker ${COMMAND} \
       ${PLATFORM_ARG} \
       ${TAG_ARG} \
       ${additional_args[@]} \
       ${CONTAINERS_DIR_ARG}
