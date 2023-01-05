include {
    start_cluster;
} from '../subworkflows/start_cluster'

include {
    stop_cluster;
} from '../subworkflows/stop_cluster'

include {
    BIGSTREAM;
} from '../modules/bigstream/main'

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

    def align_results = start_cluster()
    | combine(align_input)
    | map {
        def (cluster_id, scheduler_ip, cluster_work_dir, connected_workers,
             highres_fixed, highres_fixed_dataset,
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
            global_transform_dir,
            global_transform_name,
            '', // lowres_aligned_name
            true, // use_existing_global_transform
            highres_fixed, highres_fixed_dataset,
            highres_moving, highres_moving_dataset,
            highres_steps,
            highres_output,
            highres_transform_name,
            highres_aligned_name,
            scheduler_ip 
            scheduler_work_dir
        ]
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
          def (lowres_output_path,
               lowres_transform_name,
               lowres_aligned_name,
               highres_output_path,
               highres_transform_name,
               highres_aligned_name,
               scheduler,
               scheduler_workdir) = it
        [
            scheduler_workdir,
            scheduler,
            lowres_output_path,
            lowres_transform_name,
            lowres_aligned_name,
            highres_output_path,
            highres_transform_name,
            highres_aligned_name,
        ]
    }

    done = stop_cluster(align_results.map { it[0] })
    | join(align_results, by:0)
    | map {
        it[2..-1] // return everything except scheduler info
    }

    emit:
    done
}
