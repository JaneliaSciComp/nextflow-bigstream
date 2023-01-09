#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_params;
} from './lib/param_utils'

final_params = default_params(params)

include {
    LOCAL_BIGSTREAM_ALIGN;
} from './workflows/local-bigstream-align' addParams(final_params)

workflow {
    LOCAL_BIGSTREAM_ALIGN(Channel.of(
        [
        final_params.fixed_highres_path,
        final_params.fixed_highres_subpath,
        final_params.moving_highres_path,
        final_params.moving_highres_subpath,
        final_params.local_steps,
        final_params.local_output_path,
        final_params.local_transform_name,
        final_params.local_aligned_name,
        final_params.global_output_path,
        final_params.global_transform_name,
        ]
    ))
}
