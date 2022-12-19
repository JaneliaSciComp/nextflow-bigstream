include {
    start_cluster;
} from '../subworkflows/start_cluster'

include {
    stop_cluster;
} from '../subworkflows/stop_cluster'

workflow BIGSTREAM_REGISTRATION {
    start_cluster
    | map {
        def (cluster_id, scheduler_ip, cluster_work_dir, connected_workers) = it
        [
            cluster_work_dir,
        ]
    }
    | stop_cluster
}
