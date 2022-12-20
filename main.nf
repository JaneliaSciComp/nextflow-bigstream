#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_params;
} from './lib/param_utils'

final_params = default_params(params)

include {
    BIGSTREAM_REGISTRATION
} from './workflows/bigstream-registration' addParams(final_params)


workflow {
    log.info "!!!! " + final_params
    BIGSTREAM_REGISTRATION()
}
