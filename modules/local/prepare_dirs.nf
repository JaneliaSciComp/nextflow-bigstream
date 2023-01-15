include {
    get_runtime_opts;
    normalized_file_name;
    parentfile;
} from '../../lib/utils'

process PREPARE_DIRS {
    label 'process_low'

    container { params.bigstream_container }
    containerOptions { get_runtime_opts(ds.collect { parentfile(it) }) }

    input:
    val(ds)

    output:
    val(ds)

    script:
    def dirs = ds.collect { normalized_file_name(it) }
                .findAll { it ? true : false }
                .inject '', {acc, val -> "${acc} ${val}"}
    """
    echo "Create directories: ${dirs}"
    for d in "${dirs}"; do
        mkdir -p \$d
    done
    """
}
