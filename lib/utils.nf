def get_runtime_opts(paths) {
    def bind_paths = get_mounted_vols_opts(paths)
    "${params.runtime_opts} ${bind_paths}"
}

/**
 * index_channel converts the original channel into
 * another channel that contains a tuple of with 
 * the position of the element in the channel and element itself.
 * For example:
 * [e1, e2, e3, ..., en] -> [ [0, e1], [1, e2], [2, e3], ..., [n-1, en] ]
 *
 * This function is needed when we need to pair outputs from process, let's say P1,
 * with other inputs to be passed to another process, P2  in the pipeline,
 * because the asynchronous nature of the process execution
 */
def index_channel(c) {
    c.reduce([ 0, [] ]) { accum, elem ->
        def indexed_elem = [accum[0], elem]
        [ accum[0]+1, accum[1]+[indexed_elem] ]
    } | flatMap { it[1] }
}

def normalized_file_name(f) {
    if (f) {
        "${file(f)}"
    } else {
        ''
    }
}

def parentfile(f) {
    if (f) {
        file(f).parent
    } else {
        null
    }
}

def get_mounted_vols_opts(paths) {
    def unique_paths = paths
                        .collect { normalized_file_name(it) }
                        .findAll { it ? true : false }
                        .unique(false)
    switch (workflow.containerEngine) {
        case 'docker': 
            return unique_paths
                .collect { "-v $it:$it" }
                .join(' ')
        case 'singularity':
            return unique_paths
                .collect { "-B $it" }
                .join(' ')
        default:
            log.error "Unsupported container engine: ${workflow.containerEngine}"
            ''
    }
}
