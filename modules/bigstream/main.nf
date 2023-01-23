include {
    get_runtime_opts;
    parentfile;
    normalized_file_name;
} from '../../lib/utils'

process BIGSTREAM {
    container { params.bigstream_container }
    containerOptions { get_runtime_opts([
        lowres_fixed_path,
        lowres_moving_path,
        highres_fixed_path,
        highres_moving_path,
        parentfile(lowres_output_path),
        parentfile(highres_output_path),
        parentfile(params.local_working_path),
        parentfile(params.dask_config),
    ]) }

    memory { "${mem_gb} GB" }
    cpus { ncpus }

    input:
    tuple val(lowres_fixed_path), val(lowres_fixed_subpath),
          val(lowres_moving_path), val(lowres_moving_subpath),
          val(lowres_steps), // global steps
          val(lowres_output_path),
          val(lowres_transform_name),
          val(lowres_aligned_name),
          val(highres_fixed_path), val(highres_fixed_subpath),
          val(highres_moving_path), val(highres_moving_subpath),
          val(highres_steps), // local steps
          val(highres_output_path),
          val(highres_transform_name),
          val(highres_aligned_name)

    val(lowres_use_existing_transform)

    val(ncpus)

    val(mem_gb)

    tuple val(cluster_scheduler),
          val(cluster_workdir)

    output:
    tuple val(lowres_fixed_path), val(lowres_fixed_subpath),
          val(lowres_moving_path), val(lowres_moving_subpath),
          val(lowres_output_path),
          val(lowres_transform_name),
          val(lowres_aligned_name),
          val(highres_fixed_path), val(highres_fixed_subpath),
          val(highres_moving_path), val(highres_moving_subpath),
          val(highres_output_path),
          val(highres_transform_name),
          val(highres_aligned_name)
    tuple val(cluster_scheduler),
          val(cluster_workdir)

    script:
    def lowres_steps_arg = lowres_steps
        ? "--global-registration-steps ${lowres_steps}"
        : ''
    def lowres_fixed_args = lowres_fixed_path
        ? "--fixed-lowres ${lowres_fixed_path} --fixed-lowres-subpath ${lowres_fixed_subpath}"
        : ''
    def lowres_moving_args = lowres_moving_path
        ? "--moving-lowres ${lowres_moving_path} --moving-lowres-subpath ${lowres_moving_subpath}"
        : ''
    def lowres_output_arg = lowres_output_path
        ? "--global-output-dir ${lowres_output_path}"
        : ''
    def mk_lowres_output = lowres_output_path
        ? "mkdir -p ${lowres_output_path}"
        : ''
    def lowres_transform_name = params.global_transform_name
        ? "--global-transform-name ${params.global_transform_name}"
        : ''
    def lowres_aligned_name = params.global_aligned_name
        ? "--global-aligned-name ${params.global_aligned_name}"
        : ''
    def highres_steps_arg = highres_steps
        ? "--local-registration-steps ${highres_steps}"
        : ''
    def highres_fixed_args = highres_fixed_path
        ? "--fixed-highres ${highres_fixed_path} --fixed-highres-subpath ${highres_fixed_subpath}"
        : ''
    def highres_moving_args = highres_moving_path
        ? "--moving-highres ${highres_moving_path} --moving-highres-subpath ${highres_moving_subpath}"
        : ''
    def highres_output_arg = highres_output_path
        ? "--local-output-dir ${highres_output_path}"
        : ''
    def mk_highres_output = highres_output_path
        ? "mkdir -p ${highres_output_path}"
        : ''
    def highres_transform_name = params.local_transform_name
        ? "--local-transform-name ${params.local_transform_name}"
        : ''
    def highres_aligned_name = params.local_aligned_name
        ? "--local-aligned-name ${params.local_aligned_name}"
        : ''
    def use_existing_lowres_transform = lowres_use_existing_transform
        ? '--use-existing-global-transform'
        : ''
    def mk_highres_working_dir = params.local_working_path
        ? "mkdir -p ${normalized_file_name(params.local_working_path)}"
        : ''
    def highres_working_dir = params.local_working_path
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
    ${mk_lowres_output}
    ${mk_highres_output}
    ${mk_highres_working_dir}
    python /app/bigstream/scripts/main_align_pipeline.py \
        ${lowres_steps_arg} \
        ${lowres_fixed_args} \
        ${lowres_moving_args} \
        ${lowres_output_arg} \
        ${lowres_transform_name} \
        ${lowres_aligned_name} \
        ${use_existing_lowres_transform} \
        ${highres_steps_arg} \
        ${highres_fixed_args} \
        ${highres_moving_args} \
        ${highres_output_arg} \
        ${highres_transform_name} \
        ${highres_aligned_name} \
        ${highres_working_dir} \
        --partition-blocksize ${params.partition_blocksize} \
        --output-chunk-size ${params.output_blocksize} \
        --global-shrink-factors ${params.global_shrink_factors} \
        --global-smooth-sigmas ${params.global_smooth_sigmas} \
        --global-learning-rate ${params.global_learning_rate} \
        --global-iterations ${params.global_iterations} \
        --local-smooth-sigmas ${params.local_smooth_sigmas} \
        --local-learning-rate ${params.local_learning_rate} \
        --local-iterations ${params.local_iterations} \
        ${scheduler_arg} \
        ${dask_config_arg}
    """
}
