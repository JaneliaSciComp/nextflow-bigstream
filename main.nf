#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_params;
} from './lib/param_utils'

final_params = default_params(params)

include {
    BIGSTREAM_REGISTRATION;
} from './subworkflows/bigstream-registration' addParams(final_params)

workflow {
    def deform_input = Channel.of(get_input_deform_params(final_params.additional_deforms))
    def res = BIGSTREAM_REGISTRATION(
                Channel.of(
                    [
                        final_params.global_fixed_path,
                        final_params.global_fixed_subpath,
                        final_params.global_moving_path,
                        final_params.global_moving_subpath,
                        final_params.global_steps,
                        final_params.global_output_path,
                        final_params.global_transform_name,
                        final_params.global_aligned_name,
                        final_params.local_fixed_path,
                        final_params.local_fixed_subpath,
                        final_params.local_moving_path,
                        final_params.local_moving_subpath,
                        final_params.local_steps,
                        final_params.local_output_path,
                        final_params.local_transform_name,
                        final_params.local_inv_transform_name,
                        final_params.local_aligned_name,
                    ]),
                deform_input,
              )
    res | view
}

// multiple deform inputs are separated by ':'
// and deform input components are separated by ','
// e.g.
// vol1_path,vol1_subpath,vol1_deform_output:vol2_path,vol2_subpath,vol2_deform_output
def get_input_deform_params(deform_params) {
    if (!deform_params || !(deform_params instanceof String)) {
        return []
    }
    return deform_params.tokenize(':')
            .collect {
                it.trim()
            }
            .findAll { it }
            .collect {
                def deform_input = it.split(',').collect { it.trim() }
                def (vol_path, vol_subpath, vol_deform_output) = deform_input
                [ vol_path, vol_subpath, vol_deform_output ]
            }
}
