#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_params;
} from './lib/param_utils'

final_params = default_params(params)

include {
    TRANSFORM_COORDS;
} from './subworkflows/transform-coords' addParams(final_params)

workflow {
    def res = TRANSFORM_COORDS(
                Channel.of(
                    [
                        final_params.coords_path,
                        final_params.warped_coords_path,
                        final_params.coords_volume_path,
                        final_params.coords_volume_subpath,
                    ]),
                Channel.of(
                    [
                        "${final_params.global_output_path}/${final_params.global_transform_name}",
                        "${final_params.local_output_path}/${final_params.local_transform_name}",
                        '',
                    ]
                )
              )
    res | view
}
