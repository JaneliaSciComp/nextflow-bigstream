include {
    default_dask_params;
} from '../external-modules/dask/lib/dask_params'

def dask_params() {
    default_dask_params() +
    [
        dask_container: 'public.ecr.aws/janeliascicomp/multifish/bigstream-dask:1.2',
        with_dask_cluster: true,
    ]
}
