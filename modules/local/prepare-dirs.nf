include {
    get_runtime_opts;
    isparentfile;
    normalized_file_name;
    parentfile;
} from '../../lib/utils'

process PREPARE_DIRS {
    label 'process_low'

    container { params.bigstream_container }
    containerOptions { get_runtime_opts(combine_dirs(inputdirs, outputdirs)) }

    input:
    val(inputdirs)
    val(outputdirs)

    output:
    val(alldirsres)

    script:
    def alldirs = cleandirs(inputdirs) + cleandirs(outputdirs)
    def dirs = alldirs.inject '', {acc, val -> 
        acc ? "${acc} ${val}" : val
    }
    alldirsres = alldirs
    """
    echo "Create directories: ${dirs}"
    for d in "${dirs}"; do
        mkdir -p \$d
    done
    """
}

def combine_dirs(inputdirs, outputdirs) {
    def alldirs = [] as Set
    def inputs = inputdirs.each {
        def pf = parentfile(it, 2)
        alldirs = alldirs + [pf]
    }
    def outputs = outputdirs
                    .findAll {
                        def pf = parentfile(it, 2)
                        def existing = alldirs.find { isparentfile(it, pf) }
                        return existing == null
                    }
                    .each {
                        def pf = parentfile(it, 2)
                        alldirs = alldirs + [pf]
                    }
    log.debug "Combined dirs: $alldirs"
    alldirs
}

def cleandirs(dirs) {
    dirs.collect { normalized_file_name(it) }
        .findAll { it ? true : false }
}