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
        global_fixed_mask_path,
        global_moving_mask_path,
        local_fixed_path,
        local_moving_path,
        local_fixed_mask_path,
        local_moving_mask_path,
        parentfile(global_output_path, 2),
        parentfile(local_output_path, 2),
        parentfile(params.dask_config, 1),
    ]) }

    memory { "${mem_gb} GB" }
    cpus { ncpus }

    input:
    tuple val(global_fixed_path), val(global_fixed_subpath),
          val(global_moving_path), val(global_moving_subpath),
          val(global_fixed_mask_path), val(global_fixed_mask_subpath),
          val(global_moving_mask_path), val(global_moving_mask_subpath),
          val(global_steps), // global steps
          val(global_output_path),
          val(global_transform_name),
          val(global_aligned_name),
          val(local_fixed_path), val(local_fixed_subpath),
          val(local_moving_path), val(local_moving_subpath),
          val(local_fixed_mask_path), val(local_fixed_mask_subpath),
          val(local_moving_mask_path), val(local_moving_mask_subpath),
          val(local_steps), // local steps
          val(local_output_path),
          val(local_transform_name), val(local_transform_dataset),
          val(local_inv_transform_name), val(local_inv_transform_dataset),
          val(local_aligned_name)

    val(global_use_existing_transform)

    val(ncpus)

    val(mem_gb)

    tuple val(cluster_scheduler),
          val(cluster_workdir)

    output:
    tuple val(global_fixed_path), val(global_fixed_subpath),
          val(global_moving_path), val(global_moving_subpath),
          val(global_fixed_mask_path), val(global_fixed_mask_subpath),
          val(global_moving_mask_path), val(global_moving_mask_subpath),
          val(global_output_path),
          val(global_transform_name),
          val(global_aligned_name),
          val(local_fixed_path), val(local_fixed_subpath),
          val(local_moving_path), val(local_moving_subpath),
          val(local_fixed_mask_path), val(local_fixed_mask_subpath),
          val(local_moving_mask_path), val(local_moving_mask_subpath),
          val(local_output_path),
          val(local_transform_name), val(local_transform_dataset),
          val(local_inv_transform_name), val(local_inv_transform_dataset),
          val(local_aligned_name),
          val(cluster_scheduler), val(cluster_workdir)

    script:
    def global_steps_arg = global_steps
        ? "--global-registration-steps ${global_steps}"
        : ''
    def global_fixed_arg = global_fixed_path
        ? "--fixed-global ${global_fixed_path}"
        : ''
    def global_fixed_subpath_arg = global_fixed_subpath
        ? "--fixed-global-subpath ${global_fixed_subpath}"
        : ''
    def global_moving_arg = global_moving_path
        ? "--moving-global ${global_moving_path}"
        : ''
    def global_moving_subpath_arg = global_moving_subpath
        ? "--moving-global-subpath ${global_moving_subpath}"
        : ''
    def global_fixed_mask_arg = global_fixed_mask_path
        ? "--fixed-global-mask ${global_fixed_mask_path}"
        : ''
    def global_fixed_mask_subpath_arg = global_fixed_mask_subpath
        ? "--fixed-global-mask-subpath ${global_fixed_mask_subpath}"
        : ''
    def global_moving_mask_arg = global_moving_mask_path
        ? "--moving-global-mask ${global_moving_mask_path}"
        : ''
    def global_moving_mask_subpath_arg = global_moving_mask_subpath
        ? "--moving-global-mask-subpath ${global_moving_mask_subpath}"
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

    def global_metric_arg = params.global_metric
        ? "--global-metric ${params.global_metric}" : ''
    def global_optimizer_arg = params.global_optimizer
        ? "--global-optimizer ${params.global_optimizer}" : ''
    def global_sampling_arg = params.global_sampling
        ? "--global-sampling ${params.global_sampling}" : ''
    def global_interpolator_arg = params.global_interpolator
        ? "--global-interpolator ${params.global_interpolator}" : ''
    def global_sampling_percentage_arg = params.global_sampling_percentage
        ? "--global-sampling-percentage ${params.global_sampling_percentage}" : ''
    def global_alignment_spacing_arg = params.global_alignment_spacing
        ? "--global-alignment-spacing ${params.global_alignment_spacing}" : ''

    def mk_global_transform_path = get_mkdir_command(global_output_path, global_transform_name)
    def mk_global_aligned_path = get_mkdir_command(global_output_path, global_aligned_name)
    def local_steps_arg = local_steps
        ? "--local-registration-steps ${local_steps}"
        : ''
    def local_fixed_arg = local_fixed_path
        ? "--fixed-local ${local_fixed_path}"
        : ''
    def local_fixed_subpath_arg = local_fixed_subpath
        ? "--fixed-local-subpath ${local_fixed_subpath}"
        : ''
    def local_moving_arg = local_moving_path
        ? "--moving-local ${local_moving_path}"
        : ''
    def local_moving_subpath_arg = local_moving_subpath
        ? "--moving-local-subpath ${local_moving_subpath}"
        : ''
    def local_fixed_mask_arg = local_fixed_mask_path
        ? "--fixed-local-mask ${local_fixed_mask_path}"
        : ''
    def local_fixed_mask_subpath_arg = local_fixed_mask_subpath
        ? "--fixed-local-mask-subpath ${local_fixed_mask_subpath}"
        : ''
    def local_moving_mask_arg = local_moving_mask_path
        ? "--moving-local-mask ${local_moving_mask_path}"
        : ''
    def local_moving_mask_subpath_arg = local_moving_mask_subpath
        ? "--moving-local-mask-subpath ${local_moving_mask_subpath}"
        : ''
    def local_output_arg = local_output_path
        ? "--local-output-dir ${local_output_path}"
        : ''
    def local_transform_name_arg = local_transform_name
        ? "--local-transform-name ${local_transform_name}"
        : ''
    def local_transform_subpath_arg = local_transform_dataset
        ? "--local-transform-subpath ${local_transform_dataset}"
        : ''
    def local_transform_blocksize_arg = params.local_transform_blocksize
        ? "--local-transform-blocksize ${params.local_transform_blocksize}"
        : ''
    def local_inv_transform_name_arg = local_inv_transform_name
        ? "--local-inv-transform-name ${local_inv_transform_name}"
        : ''
    def local_inv_transform_subpath_arg = local_inv_transform_dataset
        ? "--local-inv-transform-subpath ${local_inv_transform_dataset}"
        : ''
    def local_inv_transform_blocksize_arg = params.local_inv_transform_blocksize
        ? "--local-inv-transform-blocksize ${params.local_inv_transform_blocksize}"
        : ''
    def local_aligned_name_arg = local_aligned_name
        ? "--local-aligned-name ${local_aligned_name}"
        : ''

    def local_metric_arg = params.local_metric
        ? "--local-metric ${params.local_metric}" : ''
    def local_optimizer_arg = params.local_optimizer
        ? "--local-optimizer ${params.local_optimizer}" : ''
    def local_sampling_arg = params.local_sampling
        ? "--local-sampling ${params.local_sampling}" : ''
    def local_interpolator_arg = params.local_interpolator
        ? "--local-interpolator ${params.local_interpolator}" : ''
    def local_sampling_percentage_arg = params.local_sampling_percentage
        ? "--local-sampling-percentage ${params.local_sampling_percentage}" : ''
    def local_alignment_spacing_arg = params.local_alignment_spacing
        ? "--local-alignment-spacing ${params.local_alignment_spacing}" : ''

    def mk_local_transform_path = get_mkdir_command(local_output_path, local_transform_name)
    def mk_local_inv_transform_path = get_mkdir_command(local_output_path, local_inv_transform_name)
    def mk_local_aligned_path = get_mkdir_command(local_output_path, local_aligned_name)

    def use_existing_global_transform = global_use_existing_transform
        ? '--use-existing-global-transform'
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
    python /app/bigstream/scripts/main_align_pipeline.py \
        ${global_steps_arg} \
        ${global_fixed_arg} \
        ${global_fixed_subpath_arg} \
        ${global_moving_arg} \
        ${global_moving_subpath_arg} \
        ${global_fixed_mask_arg} \
        ${global_fixed_mask_subpath_arg} \
        ${global_moving_mask_arg} \
        ${global_moving_mask_subpath_arg} \
        ${global_output_arg} \
        ${global_transform_name_arg} \
        ${global_aligned_name_arg} \
        ${use_existing_global_transform} \
        ${local_steps_arg} \
        ${local_fixed_arg} \
        ${local_fixed_subpath_arg} \
        ${local_moving_arg} \
        ${local_moving_subpath_arg} \
        ${local_fixed_mask_arg} \
        ${local_fixed_mask_subpath_arg} \
        ${local_moving_mask_arg} \
        ${local_moving_mask_subpath_arg} \
        ${local_output_arg} \
        ${local_transform_name_arg} \
        ${local_transform_subpath_arg} \
        ${local_inv_transform_name_arg} \
        ${local_inv_transform_subpath_arg} \
        ${local_aligned_name_arg} \
        --blocks-overlap-factor ${params.local_overlap_factor} \
        --output-blocksize ${params.local_blocksize} \
        ${local_transform_blocksize_arg} \
        ${local_inv_transform_blocksize_arg} \
        --global-shrink-factors ${params.global_shrink_factors} \
        --global-ransac-num-sigma-max ${params.global_ransac_num_sigma_max} \
        --global-ransac-cc-radius ${params.global_ransac_cc_radius} \
        --global-ransac-nspots ${params.global_ransac_nspots} \
        --global-ransac-diagonal-constraint ${params.global_ransac_diagonal_constraint} \
        --global-ransac-match-threshold ${params.global_ransac_match_threshold} \
        --global-ransac-align-threshold ${params.global_ransac_align_threshold} \
        --global-ransac-fix-spot-detection-threshold ${params.global_ransac_fix_spot_detection_threshold} \
        --global-ransac-fix-spot-detection-threshold-rel ${params.global_ransac_fix_spot_detection_threshold_rel} \
        --global-ransac-mov-spot-detection-threshold ${params.global_ransac_mov_spot_detection_threshold} \
        --global-ransac-mov-spot-detection-threshold-rel ${params.global_ransac_mov_spot_detection_threshold_rel} \
        --global-ransac-blob-sizes ${params.global_ransac_blob_sizes} \
        --global-ransac-fix-spots-count-threshold ${global_ransac_fix_spots_count_threshold} \
        --global-ransac-mov-spots-count-threshold ${global_ransac_mov_spots_count_threshold} \
        --global-ransac-point-matches-threshold ${global_ransac_point_matches_threshold} \
        --global-smooth-sigmas ${params.global_smooth_sigmas} \
        --global-learning-rate ${params.global_learning_rate} \
        --global-iterations ${params.global_iterations} \
        ${global_metric_arg} \
        ${global_optimizer_arg} \
        ${global_sampling_arg} \
        ${global_interpolator_arg} \
        ${global_sampling_percentage_arg} \
        ${global_alignment_spacing_arg} \
        --local-ransac-num-sigma-max ${params.local_ransac_num_sigma_max} \
        --local-ransac-cc-radius ${params.local_ransac_cc_radius} \
        --local-ransac-nspots ${params.local_ransac_nspots} \
        --local-ransac-diagonal-constraint ${params.local_ransac_diagonal_constraint} \
        --local-ransac-match-threshold ${params.local_ransac_match_threshold} \
        --local-ransac-align-threshold ${params.local_ransac_align_threshold} \
        --local-ransac-fix-spot-detection-threshold ${params.local_ransac_fix_spot_detection_threshold} \
        --local-ransac-fix-spot-detection-threshold-rel ${params.local_ransac_fix_spot_detection_threshold_rel} \
        --local-ransac-mov-spot-detection-threshold ${params.local_ransac_mov_spot_detection_threshold} \
        --local-ransac-mov-spot-detection-threshold-rel ${params.local_ransac_mov_spot_detection_threshold_rel} \
        --local-ransac-blob-sizes ${params.local_ransac_blob_sizes} \
        --local-ransac-fix-spots-count-threshold ${local_ransac_fix_spots_count_threshold} \
        --local-ransac-mov-spots-count-threshold ${local_ransac_mov_spots_count_threshold} \
        --local-ransac-point-matches-threshold ${local_ransac_point_matches_threshold} \
        --local-smooth-sigmas ${params.local_smooth_sigmas} \
        --local-learning-rate ${params.local_learning_rate} \
        --local-iterations ${params.local_iterations} \
        ${local_metric_arg} \
        ${local_optimizer_arg} \
        ${local_sampling_arg} \
        ${local_interpolator_arg} \
        ${local_sampling_percentage_arg} \
        ${local_alignment_spacing_arg} \
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
