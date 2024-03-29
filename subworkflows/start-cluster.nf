include {
    CREATE_DASK_CLUSTER
} from '../external-modules/dask/workflows/create_dask_cluster'

include {
    normalized_file_name;
} from '../lib/utils'

include {
    PREPARE_DIRS;
} from '../modules/local/prepare-dirs'

workflow START_CLUSTER {
    take:
    cluster_input_paths
    cluster_output_paths

    main:
    if (params.with_dask_cluster) {
        def accessible_paths = PREPARE_DIRS(cluster_input_paths,
                                            cluster_output_paths)
        accessible_paths.subscribe { log.debug "Cluster paths: $it" }
        cluster = CREATE_DASK_CLUSTER(
            file(params.dask_work_dir),
            accessible_paths
        )
    } else {
        cluster = Channel.of(['', '', params.dask_work_dir, -1]).combine(cluster_input_paths.ifEmpty(''))
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
