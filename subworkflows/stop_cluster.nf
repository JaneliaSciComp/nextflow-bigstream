workflow stop_cluster {
    take:
    work_dir

    main:
    if (params.with_dask_cluster) {
        done = DASK_CLUSTER_TERMINATE(work_dir)
    } else {
        done = work_dir
    }

    emit:
    work_dir
}
