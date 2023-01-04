include {
    start_cluster;
} from '../subworkflows/start_cluster'

include {
    stop_cluster;
} from '../subworkflows/stop_cluster'

include {
    BIGSTREAM;
} from '../modules/bigstream/main'

workflow BIGSTREAM_REGISTRATION {
    take:
    registration_input // [fixed_lowres, fixed_lowres_dataset, moving_lowres, moving_lowres_dataset, 
                       //  fixed_highres, fixed_highres_dataset, moving_highres, moving_highres_dataset,
                       //  output, output_dataset]

    main:

    start_cluster()
    | combine(registration_input)
    | map {
        def (cluster_id, scheduler_ip, cluster_work_dir, connected_workers,
             fixed_lowres, fixed_lowres_dataset, moving_lowres, moving_lowres_dataset,
             fixed_highres, fixed_highres_dataset, moving_highres, moving_highres_dataset,
             output, output_dataset) = it
        [
            fixed_lowres, fixed_lowres_dataset,
            moving_lowres, moving_lowres_dataset,
            fixed_highres, fixed_highres_dataset,
            moving_highres, moving_highres_dataset,
            output, output_dataset,
            scheduler_ip, cluster_work_dir
        ]
    }
    | BIGSTREAM
    | map {
        def (output, output_dataset,
            scheduler_ip, cluster_work_dir) = it
        return cluster_work_dir
    }
    | stop_cluster
}
