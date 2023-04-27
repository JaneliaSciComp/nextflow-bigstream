include {
    get_runtime_opts;
    parentfile;
    normalized_file_name;
} from '../../lib/utils'

process LOCAL_TRANSFORM {
    container { params.bigstream_container }
    containerOptions { get_runtime_opts([
        fixed_path,
        moving_path,
        parentfile(output_path, 1),
        parentfile(global_transform, 1),
        parentfile(local_transform, 1),
        parentfile(params.local_working_path, 2),
        parentfile(params.dask_config, 1)]) }

    memory { "${mem_gb} GB" }
    cpus { ncpus }

    input:
    tuple val(fixed_path), val(fixed_subpath),
          val(moving_path), val(moving_subpath),
          val(output_path), val(output_subpath),
          val(global_transform),
          val(local_transform_path), val(local_transform_subpath)
    tuple val(cluster_scheduler),
          val(cluster_workdir)
    val(ncpus)
    val(mem_gb)

    output:
    tuple val(fixed_path), val(fixed_subpath),
          val(moving_path), val(moving_subpath),
          val(output_path), val(output_subpath),
          val(cluster_scheduler), val(cluster_workdir)

    script:
    def parent_output = file(output_path).parent
    def output_subpath_arg = output_subpath
        ? "--output-subpath ${output_subpath}"
        : ''
    def affine_transforms_arg = global_transform
        ? "--affine-transformations ${global_transform}"
        : ''
    def local_transform_arg = local_transform_path
        ? "--local-transform ${local_transform_path}"
        : ''
    def local_transform_subpath_arg = local_transform_subpath
        ? "--local-transform-subpath ${local_transform_subpath}"
        : ''

    def mk_working_dir = params.local_working_path
        ? "mkdir -p ${normalized_file_name(params.local_working_path)}"
        : ''
    def working_dir_arg = params.local_working_path
        ? "--working-dir ${normalized_file_name(params.local_working_path)}"
        : ''
    def scheduler_arg = cluster_scheduler
        ? "--dask-scheduler ${cluster_scheduler}"
        : ''
    def dask_config_arg = params.dask_config
        ? "--dask-config ${normalized_file_name(params.dask_config)}"
        : ''
    """
    umask 0002
    mkdir -p ${parent_output}
    ${mk_working_dir}
    python /app/bigstream/scripts/main_apply_local_transform.py \
        --fixed ${fixed_path} --fixed-subpath ${fixed_subpath} \
        --moving ${moving_path} --moving-subpath ${moving_subpath} \
        --output ${output_path} ${output_subpath_arg} \
        ${affine_transforms_arg} \
        ${local_transform_arg} ${local_transform_subpath_arg} \
        ${working_dir_arg} \
        --output-chunk-size ${params.local_blocksize} \
        --partition-blocksize ${params.local_partitionsize} \
        --partition-overlap ${params.local_partition_overlap} \
        ${scheduler_arg} \
        ${dask_config_arg}

    """
}
