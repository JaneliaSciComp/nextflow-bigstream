include {
    get_runtime_opts;
    parentfile;
    normalized_file_name;
} from '../../lib/utils'

process GLOBAL_TRANSFORM {
    container { params.bigstream_container }
    containerOptions { get_runtime_opts([
        fixed_path,
        moving_path,
        parentfile(output_path, 1),
        parentfile(global_transform, 1)
    ]) }

    memory { "${mem_gb} GB" }
    cpus { ncpus }

    input:
    tuple val(fixed_path), val(fixed_subpath),
          val(moving_path), val(lowres_moving_subpath),
          val(output_path), val(output_subpath),
          val(global_transform)
    val(ncpus)
    val(mem_gb)

    output:
    tuple val(fixed_path), val(fixed_subpath),
          val(moving_path), val(moving_subpath),
          val(output_path), val(output_subpath)

    script:
    def parent_output = file(output_path).parent
    def output_subpath_arg = output_subpath
        ? "--output-subpath ${output_subpath}"
        : ''
    def transforms_arg = global_transform
        ? "--global-transformations ${global_transform}"
        : ''
    """
    umask 0002
    mkdir -p ${parent_output}
    python /app/bigstream/scripts/main_apply_global_transform.py \
        --fixed ${fixed_path} --fixed-subpath ${fixed_subpath} \
        --moving ${moving_path} --moving-subpath ${moving_subpath} \
        --output ${output_path} ${output_subpath_arg} \
        ${transforms_arg} \
        --output-chunk-size ${params.global_blocksize}
    """
}
