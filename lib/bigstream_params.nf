def bigstream_params() {
    [
        bigstream_container: 'bigstream:1.0',
        // global alignment (low res) parameters
        lowres_ransac_blob_sizes: '6,20',
        lowres_affine_sigmas: '2.5',
        lowres_affine_iterations: 400,
        // local alignment (high res) parameters
        highres_ransac_blob_sizes: '6,20',
        highres_affine_sigmas: '2.5',
        highres_affine_iterations: 25,
    ]
}
