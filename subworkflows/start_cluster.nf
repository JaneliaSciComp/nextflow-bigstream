include {
    CREATE_DASK_CLUSTER
} from '../external-modules/dask/workflows/create_dask_cluster'

workflow start_cluster {
    main:
    if (params.with_dask_cluster) {
        cluster = CREATE_DASK_CLUSTER(file(params.work_dir), [])
    } else {
        cluster = Channel.of(['', '', params.work_dir, -1])
    }

    emit:
    cluster
}
