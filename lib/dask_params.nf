include {
    default_dask_params
} from '../external-modules/dask/lib/dask_params'

def dask_params() {
    default_dask_params() +
    [
        dask_container: 'registry.int.janelia.org/multifish/bigstream-dask:1.0',
        with_dask_cluster: true,
    ]
}
