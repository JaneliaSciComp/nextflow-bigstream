def bigstream_params() {
    [
        bigstream_container: 'docker.io/janeliascicomp/bigstream:1.2.9-dask2023.10.1-py11',
        dask_config: '',
        use_existing_global_transform: false, // if global transform already exists use it
        global_steps: '', // use 'ransac,affine' to align the global volume
        global_blocksize: '128,128,128', // output block size for global volume
        global_ransac_spot_detection_method: '',
        global_ransac_num_sigma_max: 7,
        global_ransac_cc_radius: 12,
        global_ransac_nspots: 5000,
        global_ransac_diagonal_constraint: 0.75,
        global_ransac_match_threshold: 0.6,
        global_ransac_align_threshold: 2.0,
        global_ransac_fix_spot_detection_threshold: 0.001,
        global_ransac_fix_spot_detection_threshold_rel: 0.05,
        global_ransac_fix_spot_winsorize_limits: '0,0.1',
        global_ransac_mov_spot_detection_threshold: 0.001,
        global_ransac_mov_spot_detection_threshold_rel: 0.05,
        global_ransac_mov_spot_winsorize_limits: '0,0.1',
        global_ransac_blob_sizes: '6,20',
        global_ransac_fix_spots_count_threshold: 100,
        global_ransac_mov_spots_count_threshold: 100,
        global_ransac_point_matches_threshold: 50,
        global_shrink_factors: '2',
        global_smooth_sigmas: 2.5,
        global_learning_rate: 0.25,
        global_min_step: 0,
        global_iterations: 400,
        global_metric: '',
        global_optimizer: '',
        global_sampling: '',
        global_interpolator: '',
        global_sampling_percentage: '',
        global_alignment_spacing: '',
        // global volume computation resources
        bigstream_global_cpus: 1,
        bigstream_global_mem_gb: 2,
        local_steps: '', // use ransac,deform to align the chunked volume
        local_overlap_factor: 0.5,
        local_blocksize: "128,128,128",  // output block (chunk) size for zarr or N5 arrays
        local_transform_blocksize: "32,32,32",  // output block (chunk) size for local deformation
        local_inv_transform_blocksize: '',  // output block (chunk) size for local inverse deformation
        local_ransac_spot_detection_method: '',
        local_ransac_num_sigma_max: 7,
        local_ransac_cc_radius: 12,
        local_ransac_nspots: 5000,
        local_ransac_diagonal_constraint: 0.75,
        local_ransac_match_threshold: 0.6,
        local_ransac_align_threshold: 2,
        local_ransac_fix_spot_detection_threshold: 0.0001,
        local_ransac_fix_spot_detection_threshold_rel: 0.01,
        local_ransac_fix_spot_winsorize_limits: '0,0.1',
        local_ransac_mov_spot_detection_threshold: 0.0001,
        local_ransac_mov_spot_detection_threshold_rel: 0.01,
        local_ransac_mov_spot_winsorize_limits: '0,0.1',
        local_ransac_blob_sizes: '6,20',
        local_ransac_fix_spots_count_threshold: 100,
        local_ransac_mov_spots_count_threshold: 100,
        local_ransac_point_matches_threshold: 50,
        local_control_point_spacing: 50,
        control_point_levels: '1',
        local_smooth_sigmas: 0.25,
        local_learning_rate: 0.25,
        local_min_step: 0,
        local_iterations: 25,
        local_metric: '',
        local_optimizer: '',
        local_sampling: '',
        local_interpolator: '',
        local_sampling_percentage: '',
        local_alignment_spacing: '',
        inv_iterations: 10,
        inv_order: 2,
        inv_sqrt_iterations: 10,
        // chunked volume computation resources
        bigstream_local_cpus: 1,
        bigstream_local_mem_gb: 2,
        additional_deforms: '',
        deform_local_cpus: 1,
        deform_local_mem_gb: 1,
        warp_coords_partitionsize: 0,
        warp_coords_processingblock: '',
        warp_coords_cpus: 1,
        warp_coords_mem_gb: 1,
    ]
}
