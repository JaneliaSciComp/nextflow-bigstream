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
