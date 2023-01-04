include {
    DASK_CLUSTER_TERMINATE
} from '../external-modules/dask/modules/dask/cluster_terminate/main'

workflow stop_cluster {
    take:
    work_dir

    main:
    if (params.with_dask_cluster && params.local_steps) {
        done = DASK_CLUSTER_TERMINATE(work_dir)
    } else {
        done = work_dir
    }

    emit:
    work_dir
}
