def get_runtime_opts(paths) {
    def bind_paths = get_mounted_vols_opts(paths)
    "${params.runtime_opts} ${bind_paths}"
}

def get_mounted_vols_opts(paths) {
    def unique_paths = paths.findAll { it ? true : false }.unique(false)
    switch (workflow.containerEngine) {
        case 'docker': 
            return unique_paths
                .collect {
                    def f = file(it)
                    "-v $f:$f" 
                }
                .join(' ')
        case 'singularity':
            return unique_paths
                .collect {
                    def f = file(it)
                    "-B $f" 
                }
                .join(' ')
        default:
            log.error "Unsupported container engine: ${workflow.containerEngine}"
            ''
    }
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
