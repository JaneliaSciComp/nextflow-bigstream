include {
    dask_params
} from './dask_params'

include {
    bigstream_params
} from './bigstream_params'

def default_params(Map ps) {
    dask_params() +
    bigstream_params() +
    ps
}

def registration_input_params(Map ps) {
    [
        fixed_lowres_path: '',
        fixed_lowres_subpath: 'lowres',
        moving_lowres_path: '',
        moving_lowres_subpath: 'lowres',
        fixed_highres_path: '',
        fixed_highres_subpath: 'highres',
        moving_highres_path: '',
        moving_highres_subpath: 'highres',
    ] +
    ps
}