include {
    CREATE_DASK_CLUSTER
} from '../external-modules/dask/workflows/create_dask_cluster'

include {
    normalized_file_name;
} from '../lib/utils'

include {
    PREPARE_DIRS;
} from '../modules/local/prepare_dirs'

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
        | map {
            def (cluster_id, cluster_scheduler_ip, cluster_work_dir, cluster_workers) = it
            [
                cluster_id, cluster_scheduler_ip, cluster_work_dir, cluster_workers
            ]
        }
    }

    emit:
    cluster
}
