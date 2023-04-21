include {
    get_runtime_opts;
    parentfile;
    normalized_file_name;
} from '../../lib/utils'

process BIGSTREAM {
    container { params.bigstream_container }
    containerOptions { get_runtime_opts([
        global_fixed_path,
        global_moving_path,
        local_fixed_path,
        local_moving_path,
        parentfile(global_output_path, 2),
        parentfile(local_output_path, 2),
        parentfile(params.local_working_path, 2),
        parentfile(params.dask_config, 1),
    ]) }

    memory { "${mem_gb} GB" }
    cpus { ncpus }

    input:
    tuple val(global_fixed_path), val(global_fixed_subpath),
          val(global_moving_path), val(global_moving_subpath),
          val(global_steps), // global steps
          val(global_output_path),
          val(global_transform_name),
          val(global_aligned_name),
          val(local_fixed_path), val(local_fixed_subpath),
          val(local_moving_path), val(local_moving_subpath),
          val(local_steps), // local steps
          val(local_output_path),
          val(local_transform_name),
          val(local_aligned_name)

    val(global_use_existing_transform)

    val(ncpus)

    val(mem_gb)

    tuple val(cluster_scheduler),
          val(cluster_workdir)

    output:
    tuple val(global_fixed_path), val(global_fixed_subpath),
          val(global_moving_path), val(global_moving_subpath),
          val(global_output_path),
          val(global_transform_name),
          val(global_aligned_name),
          val(local_fixed_path), val(local_fixed_subpath),
          val(local_moving_path), val(local_moving_subpath),
          val(local_output_path),
          val(local_transform_name),
          val(local_aligned_name),
          val(cluster_scheduler), val(cluster_workdir)

    script:
    def global_steps_arg = global_steps
        ? "--global-registration-steps ${global_steps}"
        : ''
    def global_fixed_args = global_fixed_path
        ? "--fixed-lowres ${global_fixed_path} --fixed-lowres-subpath ${global_fixed_subpath}"
        : ''
    def global_moving_args = global_moving_path
        ? "--moving-lowres ${global_moving_path} --moving-lowres-subpath ${global_moving_subpath}"
        : ''
    def global_output_arg = global_output_path
        ? "--global-output-dir ${global_output_path}"
        : ''
    def mk_global_output = global_output_path
        ? "mkdir -p ${global_output_path}"
        : ''
    def global_transform_name = params.global_transform_name
        ? "--global-transform-name ${params.global_transform_name}"
        : ''
    def global_aligned_name = params.global_aligned_name
        ? "--global-aligned-name ${params.global_aligned_name}"
        : ''
    def local_steps_arg = local_steps
        ? "--local-registration-steps ${local_steps}"
        : ''
    def local_fixed_args = local_fixed_path
        ? "--fixed-highres ${local_fixed_path} --fixed-highres-subpath ${local_fixed_subpath}"
        : ''
    def local_moving_args = local_moving_path
        ? "--moving-highres ${local_moving_path} --moving-highres-subpath ${local_moving_subpath}"
        : ''
    def local_output_arg = local_output_path
        ? "--local-output-dir ${local_output_path}"
        : ''
    def mk_local_output = local_output_path
        ? "mkdir -p ${local_output_path}"
        : ''
    def local_transform_name = params.local_transform_name
        ? "--local-transform-name ${params.local_transform_name}"
        : ''
    def local_aligned_name = params.local_aligned_name
        ? "--local-aligned-name ${params.local_aligned_name}"
        : ''
    def use_existing_global_transform = global_use_existing_transform
        ? '--use-existing-global-transform'
        : ''
    def mk_local_working_dir = params.local_working_path
        ? "mkdir -p ${normalized_file_name(params.local_working_path)}"
        : ''
    def local_working_dir = params.local_working_path
        ? "--local-working-dir ${normalized_file_name(params.local_working_path)}"
        : ''
    def scheduler_arg = cluster_scheduler
        ? "--dask-scheduler ${cluster_scheduler}"
        : ''
    def dask_config_arg = params.dask_config
        ? "--dask-config ${normalized_file_name(params.dask_config)}"
        : ''
    """
    umask 0002
    ${mk_global_output}
    ${mk_local_output}
    ${mk_local_working_dir}
    python /app/bigstream/scripts/main_align_pipeline.py \
        ${global_steps_arg} \
        ${global_fixed_args} \
        ${global_moving_args} \
        ${global_output_arg} \
        ${global_transform_name} \
        ${global_aligned_name} \
        ${use_existing_global_transform} \
        ${local_steps_arg} \
        ${local_fixed_args} \
        ${local_moving_args} \
        ${local_output_arg} \
        ${local_transform_name} \
        ${local_aligned_name} \
        ${local_working_dir} \
        --partition-blocksize ${params.local_partitionsize} \
        --output-chunk-size ${params.local_blocksize} \
        --global-shrink-factors ${params.global_shrink_factors} \
        --global-smooth-sigmas ${params.global_smooth_sigmas} \
        --global-learning-rate ${params.global_learning_rate} \
        --global-iterations ${params.global_iterations} \
        --local-smooth-sigmas ${params.local_smooth_sigmas} \
        --local-learning-rate ${params.local_learning_rate} \
        --local-iterations ${params.local_iterations} \
        --local-write-group-interval ${params.local_write_group_interval} \
        ${scheduler_arg} \
        ${dask_config_arg}
    """
}
