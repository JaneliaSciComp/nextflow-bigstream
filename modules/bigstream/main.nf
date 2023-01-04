include {
    get_runtime_opts
} from '../../lib/utils'

process BIGSTREAM {
    container { params.bigstream_container }
    containerOptions { get_runtime_opts([
        fixed_lowres_path,
        moving_lowres_path,
        fixed_highres_path,
        moving_highres_path,
        file(output_path).parent
    ]) }

    memory { "${params.bigstream_mem_gb} GB" }
    cpus { params.bigstream_cpus }

    input:
    tuple val(fixed_lowres_path), val(fixed_lowres_subpath),
          val(moving_lowres_path), val(moving_lowres_subpath),
          val(fixed_highres_path), val(fixed_highres_subpath),
          val(moving_highres_path), val(moving_highres_subpath),
          val(output_path), 
          val(global_steps),
          val(global_transform_name),
          val(global_aligned_name),
          val(local_steps),
          val(local_transform_name),
          val(local_aligned_name),
          val(scheduler),
          val(scheduler_workdir)

    output:
    tuple val(output_path),
          val(global_transform_name),
          val(global_aligned_name),
          val(local_transform_name),
          val(local_aligned_name),
          val(scheduler),
          val(scheduler_workdir)

    script:
    def global_steps_arg = global_steps
        ? "--global-registration-steps ${global_steps}"
        : ''
    def fixed_lowres_args = fixed_lowres_path
        ? "--fixed-lowres ${fixed_lowres_path} --fixed-lowres-subpath ${fixed_lowres_subpath}"
        : ''
    def moving_lowres_args = moving_lowres_path
        ? "--moving-lowres ${moving_lowres_path} --moving-lowres-subpath ${moving_lowres_subpath}"
        : ''
    def local_steps_arg = local_steps
        ? "--local-registration-steps ${local_steps}"
        : ''
    def fixed_highres_args = fixed_highres_path
        ? "--fixed-highres ${fixed_highres_path} --fixed-highres-subpath ${fixed_highres_subpath}"
        : ''
    def moving_highres_args = moving_highres_path
        ? "--moving-highres ${moving_highres_path} --moving-highres-subpath ${moving_highres_subpath}"
        : ''
    def global_transform_name = params.global_transform_name
        ? "--global-transform-name ${params.global_transform_name}"
        : ''
    def global_aligned_name = params.global_aligned_name
        ? "--global-aligned-name ${params.global_aligned_name}"
        : ''
    def local_transform_name = params.local_transform_name
        ? "--local-transform-name ${params.local_transform_name}"
        : ''
    def local_aligned_name = params.local_aligned_name
        ? "--local-aligned-name ${params.local_aligned_name}"
        : ''
    def scheduler_arg = scheduler
        ? "--dask-scheduler ${scheduler}"
        : ''
    def use_existing_global_transform = params.global_use_existing_transform
        ? '--use-existing-global-transform'
        : ''
    """
    umask 0002
    mkdir -p ${output_path}
    python /app/bigstream/scripts/main_pipeline.py \
        ${global_steps_arg} \
        ${fixed_lowres_args} \
        ${moving_lowres_args} \
        ${global_transform_name} \
        ${global_aligned_name} \
        ${use_existing_global_transform} \
        ${local_steps_arg} \
        ${fixed_highres_args} \
        ${moving_highres_args} \
        ${local_transform_name} \
        ${local_aligned_name} \
        --output-dir ${output_path} \
        --partition-blocksize ${params.partition_blocksize} \
        --global-shrink-factors ${params.global_shrink_factors} \
        --global-smooth-sigmas ${params.global_smooth_sigmas} \
        --global-learning-rate ${params.global_learning_rate} \
        --global-iterations ${params.global_iterations} \
        --local-smooth-sigmas ${params.local_smooth_sigmas} \
        --local-learning-rate ${params.local_learning_rate} \
        --local-iterations ${params.local_iterations} \
        ${scheduler_arg}
    """
}
