#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_params;
    registration_input_params;
} from './lib/param_utils'

final_params = default_params(params)

include {
    GLOBAL_BIGSTREAM_REGISTRATION;
} from './workflows/global-bigstream-registration' addParams(final_params)

registration_inputs = registration_input_params(params)

workflow {
    GLOBAL_BIGSTREAM_REGISTRATION(Channel.of(
        [
        file(registration_inputs.fixed_lowres_path),
        registration_inputs.fixed_lowres_subpath,
        file(registration_inputs.moving_lowres_path),
        registration_inputs.moving_lowres_subpath,
        params.global_steps,
        file(registration_inputs.output_path), 
        registration_inputs.global_transform_name,
        registration_inputs.global_aligned_name,
        ]
    ))
}
