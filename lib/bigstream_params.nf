def bigstream_params() {
    [
        bigstream_container: 'registry.int.janelia.org/multifish/bigstream-dask:1.0',
        dask_config: '',
        use_existing_global_transform: false, // if global transform already exists use it
        global_steps: '', // use 'ransac,affine' to align the global volume
        global_blocksize: 128, // output block size for global volume
        global_ransac_num_sigma_max: 15,
        global_ransac_cc_radius: 12,
        global_ransac_nspots: 5000,
        global_ransac_diagonal_constraint: 0.75,
        global_ransac_match_threshold: 0.9,
        global_ransac_align_threshold: 2.5,
        global_ransac_blob_sizes: '6,20',
        global_shrink_factors: '2',
        global_smooth_sigmas: 2.5,
        global_learning_rate: 0.25,
        global_min_step: 0,
        global_iterations: 400,
        // global volume computation resources
        bigstream_global_cpus: 1,
        bigstream_global_mem_gb: 2,
        local_working_path: '',
        local_steps: '', // use ransac,deform to align the chunked volume
        local_partitionsize: 128, // processing blocksize for parallelization
        local_partition_overlap: 0.5,
        local_blocksize: 128,  // output block (chunk) size for zarr or N5 arrays
        local_write_group_interval: 30,
        local_ransac_num_sigma_max: 15,
        local_ransac_cc_radius: 12,
        local_ransac_nspots: 5000,
        local_ransac_diagonal_constraint: 0.75,
        local_ransac_match_threshold: 0.9,
        local_ransac_align_threshold: 2.5,
        local_ransac_blob_sizes: '6,20',
        local_control_point_spacing: 50,
        control_point_levels: '1',
        local_smooth_sigmas: 0.25,
        local_learning_rate: 0.25,
        local_min_step: 0,
        local_iterations: 25,
        // chunked volume computation resources
        bigstream_local_cpus: 1,
        bigstream_local_mem_gb: 2,
        additional_deforms: '',
        deform_local_cpus: 1,
        deform_local_mem_gb: 1,
        warp_coords_partitionsize: 0,
        warp_coords_cpus: 1,
        warp_coords_mem_gb: 1,
    ]
}
