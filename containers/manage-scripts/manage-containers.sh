helpmsg="$0
    <command>{build|mp-build|push|--help}
    [-n]
    containers/bigstream
"

RUNNER=

VERSION=1.2
DOCKERHUB_TAG=multifish/bigstream-dask:${VERSION}
INTERNAL_TAG=registry.int.janelia.org/multifish/bigstream-dask:${VERSION}
ECR_TAG=public.ecr.aws/janeliascicomp/multifish/bigstream-dask:${VERSION}

args=()

while [[ $# > 0 ]]; do
    key="$1"
    shift # past the key
    case ${key} in
        -cmd)
            COMMAND=$1
            shift
            ;;
        -n)
            RUNNER=echo
            ;;
        --help|-h)
            echo ${helpmsg}
            exit 1
            ;;
        *)
            args=(${args[@]} $key)
            ;;
    esac
done

function display_tags {
    echo "AWS ECR: ${ECR_TAG}"
    echo "DockerIO: ${DOCKERHUB_TAG}"
    echo "Internal: ${INTERNAL_TAG}"
}

function docker_build {
    $RUNNER docker build \
            --platform linux/amd64 \
            $*
}

function docker_multiplatform_build {
    $RUNNER docker buildx build \
            --platform linux/amd64,linux/arm64 \
            --push \
            $*
}

function docker_push {
    $RUNNER docker push \
            $*
}

case ${COMMAND} in
    build)
        docker_build ${args[@]}
        ;;
    mp-build)
        docker_multiplatform_build ${args[@]}
        ;;
    push)
        docker_push ${args[@]}
        ;;
    display-tags)
        display_tags
        ;;
    *)
        echo ${helpmsg}
        exit 1
        ;;
esac
