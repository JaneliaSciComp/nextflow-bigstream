helpmsg="$0
    build|push|--help
"

RUNNER=

TAG_ARG="-t registry.int.janelia.org/multifish/bigstream-dask:1.2"

additional_args=()

while [[ $# > 0 ]]; do
    key="$1"
    shift # past the key
    case ${key} in
        build)
            COMMAND="build"
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
        -t)
	    TAG=$1
	    shift
            TAG_ARG="-t $TAG $TAG_ARG"
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
