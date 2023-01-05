#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_params;
} from './lib/param_utils'

final_params = default_params(params)

include {
    GLOBAL_BIGSTREAM_ALIGN;
} from './workflows/global-bigstream-align' addParams(final_params)

workflow {
    GLOBAL_BIGSTREAM_ALIGN(Channel.of(
        [
        final_params.fixed_lowres_path,
        final_params.fixed_lowres_subpath,
        final_params.moving_lowres_path,
        final_params.moving_lowres_subpath,
        final_params.global_steps,
        final_params.global_output_path,
        final_params.global_transform_name,
        final_params.global_aligned_name,
        ]
    ))
}
