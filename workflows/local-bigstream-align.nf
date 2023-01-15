include {
    start_cluster;
} from '../subworkflows/start_cluster'

include {
    stop_cluster;
} from '../subworkflows/stop_cluster'

include {
    BIGSTREAM;
} from '../modules/bigstream/main'

include {
    normalized_file_name;
} from '../lib/utils'

workflow LOCAL_BIGSTREAM_ALIGN {
    take:
    align_input // [highres_fixed, highres_fixed_dataset,
                //  highres_moving, highres_moving_dataset,
                //  highres_steps,
                //  highres_output,
                //  highres_transform_name,
                //  highres_aligned_name
                //  global_transform_dir,
                //  global_transform_name]

    main:

    def bigstream_input = align_input
    | map {
        def (highres_fixed, highres_fixed_dataset,
             highres_moving, highres_moving_dataset,
             highres_steps,
             highres_output,
             highres_transform_name,
             highres_aligned_name,
             global_transform_dir,
             global_transform_name) = it
        [
            '', '', // lowres_fixed, lowres_fixed_dataset
            '', '', // lowres_moving, lowres_moving_dataset
            '', // lowres_steps,
            normalized_file_name(global_transform_dir),
            global_transform_name,
            '', // lowres_aligned_name
            normalized_file_name(highres_fixed), highres_fixed_dataset,
            normalized_file_name(highres_moving), highres_moving_dataset,
            highres_steps,
            normalized_file_name(highres_output),
            highres_transform_name,
            highres_aligned_name,
        ]
    }

    def cluster_info = bigstream_input.map {
        def (lowres_fixed, lowres_fixed_dataset,
             lowres_moving, lowres_moving_dataset,
             lowres_steps,
             global_transform_dir,
             global_transform_name,
             lowres_aligned_name,
             highres_fixed, highres_fixed_dataset,
             highres_moving, highres_moving_dataset,
             highres_steps,
             highres_output,
             highres_transform_name,
             highres_aligned_name) = it
        [
            normalized_file_name(highres_fixed),
            normalized_file_name(highres_moving),
            normalized_file_name(global_transform_dir),
            normalized_file_name(highres_output),
            normalized_file_name(params.local_working_path),
        ]
    }
    | start_cluster
    | map {
        def (cluster_id, cluster_scheduler_ip, cluster_work_dir, connected_workers) = it
        [
            cluster_scheduler_ip, cluster_work_dir,
        ]
    }

    def bigstream_results = BIGSTREAM(bigstream_input,
                                      true, // use_existing_global_transform
                                      params.bigstream_highres_cpus,
                                      params.bigstream_highres_mem_gb,
                                      cluster_info)

    stop_cluster(bigstream_results[1].map{ it[1] })

    emit:
    done = bigstream_results[0]
}
