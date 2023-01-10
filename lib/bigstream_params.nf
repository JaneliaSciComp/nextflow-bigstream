def bigstream_params() {
    [
        bigstream_container: 'registry.int.janelia.org/multifish/bigstream-dask:1.0',
        // global alignment (low res) parameters
        fixed_lowres_path: '',
        fixed_lowres_subpath: 'lowres',
        moving_lowres_path: '',
        moving_lowres_subpath: 'lowres',
        global_output_path: '',
        global_transform_name: 'affine.mat',
        global_aligned_name: '',
        use_existing_global_transform: false, // if global transform already exists use it
        global_steps: '', // use 'ransac,affine' to run global alignment
        global_ransac_blob_sizes: '6,20',
        global_shrink_factors: '2',
        global_smooth_sigmas: 2.5,
        global_learning_rate: 0.25,
        global_min_step: 0,
        global_iterations: 400,
        // local alignment (high res) parameters
        fixed_highres_path: '',
        fixed_highres_subpath: 'highres',
        moving_highres_path: '',
        moving_highres_subpath: 'highres',
        local_output_path: '',
        local_working_path: '',
        local_transform_name: '',
        local_aligned_name: '',
        local_steps: '', // use ransac,deform to run local alignment
        partition_blocksize: 128, // processing blocksize for parallelization
        output_blocksize: 128,  // output block (chunk) size
        local_ransac_blob_sizes: '6,20',
        local_control_point_spacing: 50,
        control_point_levels: '1',
        local_smooth_sigmas: 0.25,
        local_learning_rate: 0.25,
        local_min_step: 0,
        local_iterations: 25,
        bigstream_cpus: 1,
        bigstream_mem_gb: 2,
    ]
}
