include {
    BIGSTREAM_REGISTRATION;
} from './subworkflows/local-bigstream-align'

workflow REGISTRATION {
    take:
    registration_input // [global_fixed, global_fixed_dataset,
                       //  global_moving, global_moving_dataset,
                       //  global_steps,
                       //  global_output,
                       //  global_transform_name,
                       //  global_aligned_name,
                       //  local_fixed, local_fixed_dataset,
                       //  local_moving, local_moving_dataset,
                       //  local_steps,
                       //  local_output,
                       //  local_transform_name,
                       //  local_aligned_name]
    warping_inputs // [volume, volume_dataset]
    main:

    def registration_results = BIGSTREAM_REGISTRATION(registration_input)

    emit:
    done = registration_results
}
