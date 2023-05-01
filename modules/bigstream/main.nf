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
          val(local_inv_transform_name),
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
          val(local_inv_transform_name),
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
    def global_transform_name_arg = global_transform_name
        ? "--global-transform-name ${global_transform_name}"
        : ''
    def global_aligned_name_arg = global_aligned_name
        ? "--global-aligned-name ${global_aligned_name}"
        : ''
    def mk_global_transform_path = get_mkdir_command(global_output_path, global_transform_name)
    def mk_global_aligned_path = get_mkdir_command(global_output_path, global_aligned_name)
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
    def local_transform_name_arg = local_transform_name
        ? "--local-transform-name ${local_transform_name}"
        : ''
    def local_inv_transform_name_arg = local_inv_transform_name
        ? "--local-inv-transform-name ${local_inv_transform_name}"
        : ''
    def local_aligned_name_arg = local_aligned_name
        ? "--local-aligned-name ${local_aligned_name}"
        : ''
    def mk_local_transform_path = get_mkdir_command(local_output_path, local_transform_name)
    def mk_local_inv_transform_path = get_mkdir_command(local_output_path, local_inv_transform_name)
    def mk_local_aligned_path = get_mkdir_command(local_output_path, local_aligned_name)

    def use_existing_global_transform = global_use_existing_transform
        ? '--use-existing-global-transform'
        : ''
    def mk_local_working_dir = params.local_working_path
        ? "mkdir -p ${normalized_file_name(params.local_working_path)}"
        : ''
    def local_working_dir_arg = params.local_working_path
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
    ${mk_global_transform_path}
    ${mk_global_aligned_path}
    ${mk_local_transform_path}
    ${mk_local_inv_transform_path}
    ${mk_local_aligned_path}
    ${mk_local_working_dir}
    python /app/bigstream/scripts/main_align_pipeline.py \
        ${global_steps_arg} \
        ${global_fixed_args} \
        ${global_moving_args} \
        ${global_output_arg} \
        ${global_transform_name_arg} \
        ${global_aligned_name_arg} \
        ${use_existing_global_transform} \
        ${local_steps_arg} \
        ${local_fixed_args} \
        ${local_moving_args} \
        ${local_output_arg} \
        ${local_transform_name_arg} \
        ${local_inv_transform_name_arg} \
        ${local_aligned_name_arg} \
        ${local_working_dir_arg} \
        --partition-blocksize ${params.local_partitionsize} \
        --overlap-factor ${params.local_overlap_factor} \
        --output-chunk-size ${params.local_blocksize} \
        --global-shrink-factors ${params.global_shrink_factors} \
        --global-ransac-num-sigma-max ${params.global_ransac_num_sigma_max} \
        --global-ransac-cc-radius ${params.global_ransac_cc_radius} \
        --global-ransac-nspots ${params.global_ransac_nspots} \
        --global-ransac-diagonal-constraint ${params.global_ransac_diagonal_constraint} \
        --global-ransac-match-threshold ${params.global_ransac_match_threshold} \
        --global-ransac-align-threshold ${params.global_ransac_align_threshold} \
        --global-ransac-blob-sizes ${params.global_ransac_blob_sizes} \
        --global-smooth-sigmas ${params.global_smooth_sigmas} \
        --global-learning-rate ${params.global_learning_rate} \
        --global-iterations ${params.global_iterations} \
        --local-ransac-num-sigma-max ${params.local_ransac_num_sigma_max} \
        --local-ransac-cc-radius ${params.local_ransac_cc_radius} \
        --local-ransac-nspots ${params.local_ransac_nspots} \
        --local-ransac-diagonal-constraint ${params.local_ransac_diagonal_constraint} \
        --local-ransac-match-threshold ${params.local_ransac_match_threshold} \
        --local-ransac-align-threshold ${params.local_ransac_align_threshold} \
        --local-ransac-blob-sizes ${params.local_ransac_blob_sizes} \
        --local-smooth-sigmas ${params.local_smooth_sigmas} \
        --local-learning-rate ${params.local_learning_rate} \
        --local-iterations ${params.local_iterations} \
        --local-write-group-interval ${params.local_write_group_interval} \
        --inv-iterations ${params.inv_iterations} \
        --inv-order ${params.inv_order} \
        --inv-sqrt-iterations ${params.inv_sqrt_iterations} \
        ${scheduler_arg} \
        ${dask_config_arg}
    """
}

def get_mkdir_command(dirname, fname) {
    if (dirname && fname) {
        def fullpath = new File(dirname, fname)
        "mkdir -p ${file(fullpath).parent}"
    } else if (dirname) {
        "mkdir -p ${file(dirname)}"
    } else if (fname && file(fname).parent != null) {
        "mkdir -p ${file(fname).parent}"
    } else {
        ''
    }
}
