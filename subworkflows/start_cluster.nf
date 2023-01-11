include {
    CREATE_DASK_CLUSTER
} from '../external-modules/dask/workflows/create_dask_cluster'

include {
    normalized_file_name;
} from '../lib/utils'

workflow start_cluster {
    take:
    cluster_accessible_paths

    main:
    def accessible_paths = PREPARE_DIRS(cluster_accessible_paths)

    if (params.with_dask_cluster && params.local_steps) {
        cluster = CREATE_DASK_CLUSTER(
            file(params.work_dir),
            accessible_paths
        )
    } else {
        cluster = Channel.of(['', '', params.work_dir, -1]).combine(accessible_paths)
    }

    emit:
    cluster
}
