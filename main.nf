#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_params;
    registration_input_params;
} from './lib/param_utils'

final_params = default_params(params)

include {
    BIGSTREAM_REGISTRATION
} from './workflows/bigstream-registration' addParams(final_params)

registration_inputs = registration_input_params(params)

workflow {
    log.info "!!!! " + final_params
    BIGSTREAM_REGISTRATION(Channel.of(
        [
        file(registration_inputs.fixed_lowres_path), registration_inputs.fixed_lowres_subpath,
        file(registration_inputs.moving_lowres_path), registration_inputs.moving_lowres_subpath,
        file(registration_inputs.fixed_highres_path), registration_inputs.fixed_highres_subpath,
        file(registration_inputs.moving_highres_path), registration_inputs.moving_highres_subpath,
        file(registration_inputs.output_path), registration_inputs.output_subpath,
        ]
    ))
}
