workflow start_cluster {
    main:
    if (params.with_dask_cluster) {
        cluster = CREATE_DASK_CLUSTER(file(params.work_dir), [file(params.inputPath)])
    } else {
        cluster = Channel.of(['', '', params.work_dir, -1])
    }

    emit:
    cluster
}
