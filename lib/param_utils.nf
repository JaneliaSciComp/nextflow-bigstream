include {
    dask_params;
} from './dask_params'

include {
    bigstream_params;
} from './bigstream_params'

def input_params() {
    [
        // global alignment (low res) parameters
        global_fixed_path: '',
        global_fixed_subpath: 'lowres',
        global_moving_path: '',
        global_moving_subpath: 'lowres',
        global_output_path: '',
        global_transform_name: 'affine.mat',
        global_aligned_name: '',
        // local alignment (high res) parameters
        local_fixed_path: '',
        local_fixed_subpath: 'highres',
        local_moving_path: '',
        local_moving_subpath: 'highres',
        local_output_path: '',
        local_transform_name: '',
        local_aligned_name: '',
    ]
}

def default_params(Map ps) {
    dask_params() +
    bigstream_params() +
    input_params() +
    ps
}
