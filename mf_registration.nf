include {
    BIGSTREAM_REGISTRATION;
} from './subworkflows/local-bigstream-align'

workflow REGISTRATION {
    take:
    registration_input // [0 - fixed_acq name,
                       //  1 - fixed image
                       //  2 - moving_acq name
                       //  3 - moving image
                       //  4 - output dir]
    reg_ch // registered image channel (value)
    xy_stride // xy_stride - ignored - bigstream will use params.local_partitionsize
    xy_overlap // ignored
    z_stride // ignored - will use the same value as xy_stride
    z_overlap // ignored
    affine_scale
    deformation_scale
    spots_cc_radius
    spots_spot_number
    ransac_cc_cutoff
    ransac_dist_threshold
    deform_iterations
    deform_auto_mask
    warped_channels

    main:

    def bigstream_ri = registration_input
    | map {
        def (fixed_acq_name,
             fixed,
             moving_acq_name,
             moving,
             output) = it
        // registration input
        def ri =  [
            fixed, // global_fixed
            "${reg_ch}/${affine_scale}", // global_fixed_subpath
            moving, // global_moving
            "${reg_ch}/${affine_scale}", // global_moving_subpath
            params.global_steps,
            output,
            "aff/ransac_affine.mat", // global_transform_name
            "aff/ransac_affine",     // global_aligned_name
            fixed, // local_fixed
            "${reg_ch}/${deformation_scale}", // local_fixed_subpath
            moving, // local_moving
            "${reg_ch}/${deformation_scale}", // local_moving_subpath
            params.local_steps,
            output,
            "transform",  // local_transform_name
            "warped", // local_aligned_name
        ]
        // additional deformation input
        def additional_deforms = warped_channels.collect { warped_ch ->
            [
                fixed,
                "${warped_ch}/${deformation_scale}",
                "${output}/warped"
            ]
        }
        [
            ri,
            additional_deforms
        ]
    }

    emit:
    done = registration_results
}
