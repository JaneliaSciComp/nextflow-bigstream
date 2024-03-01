include {
    default_dask_params;
} from '../external-modules/dask/lib/dask_params'

def dask_params() {
    default_dask_params() +
    [
        dask_container: 'docker.io/janeliascicomp/bigstream:1.2.9-dask2023.10.1-py11',
        with_dask_cluster: true,
    ]
}
