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
                        final_params.local_aligned_name,
                    ]),
                Channel.of(
                    [
                        [ final_params.local_moving_path, final_params.local_moving_subpath, "testwarp" ],
                    ],
                ),
              )
    res | view
}
