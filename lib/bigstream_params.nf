def bigstream_params() {
    [
        bigstream_container: 'bigstream:1.0',
        // global alignment (low res) parameters
        global_transform_name: 'affine.mat',
        global_aligned_name: '',
        global_use_existing_transform: false, // if global transform already exists use it
        global_steps: '', // use 'ransac,affine' to run global alignment
        global_ransac_blob_sizes: '6,20',
        global_shrink_factors: '2',
        global_smooth_sigmas: 2.5,
        global_learning_rate: 0.25,
        global_min_step: 0,
        global_iterations: 400,
        // local alignment (high res) parameters
        partition_blocksize: 128,
        local_transform_name: '',
        local_aligned_name: '',
        local_steps: '', // use ransac,deform to run local alignment
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
