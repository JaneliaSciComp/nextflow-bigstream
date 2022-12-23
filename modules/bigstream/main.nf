include {
    get_runtime_opts
} from '../../lib/utils'

process BIGSTREAM {
    container { params.container }
    containerOptions { get_runtime_opts([
        fixed_lowres_path,
        moving_lowres_path,
        fixed_highres_path,
        moving_highres_path,
        output_path
    ]) }

    memory { "${params.mem_gb} GB" }
    cpus { params.cpus }

    input:
    tuple val(fixed_lowres_path), val(fixed_lowres_subpath),
          val(moving_lowres_path), val(moving_lowres_subpath),
          val(fixed_highres_path), val(fixed_highres_subpath),
          val(moving_highres_path), val(moving_highres_subpath),
          val(output_path), val(output_subpath),
          val(scheduler),
          val(scheduler_workdir)

    output:
    tuple val(output_path),
          val(output_subpath),
          val(scheduler),
          val(scheduler_workdir)

    script:
    def scheduler_arg = scheduler
        ? "--dask-scheduler ${scheduler}"
        : ''
    def use_existing_global_transform = params.global_use_existing_transform
        ? '--use-existing-global-transform'
        : ''
    """
    python /app/bigstream/scripts/main_pipeline.py \
        --fixed-lowres ${fixed_lowres_path} \
        --fixed-lowres-subpath ${fixed_lowres_subpath} \
        --fixed-highres-subpath ${fixed_highres_path} \
        --moving-lowres ${moving_lowres_path} \
        --moving-lowres-subpath ${moving_lowres_subpath} \
        --moving-highres-subpath ${moving_highres_subpath} \
        --output-dir ${output_path} \
        ${use_existing_global_transform} \
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
