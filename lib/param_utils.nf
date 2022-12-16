include {
    default_dask_params
} from '../external-modules/dask/lib/dask_params'

include {
    get_mounted_vols_opts
} from './utils'

def dask_params() {
    default_dask_params() +
    [
        container: 'bigstream:1.0',
    ]
}
