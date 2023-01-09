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

workflow GLOBAL_BIGSTREAM_ALIGN {
    take:
    align_input // [lowres_fixed, lowres_fixed_dataset,
                //  lowres_moving, lowres_moving_dataset,
                //  lowres_steps,
                //  lowres_output,
                //  lowres_transform_name,
                //  lowres_aligned_name]

    main:
    // global alignment does not require a dask cluster
    done = align_input
    | map {
        log.info "Run global registration with: $it"
        def (lowres_fixed, lowres_fixed_dataset,
             lowres_moving, lowres_moving_dataset,
             lowres_steps,
             lowres_output,
             lowres_transform_name,
             lowres_aligned_name) = it
        [
            normalized_file_name(lowres_fixed), lowres_fixed_dataset,
            normalized_file_name(lowres_moving), lowres_moving_dataset,
            lowres_steps,
            normalized_file_name(lowres_output),
            lowres_transform_name,
            lowres_aligned_name,
            params.use_existing_global_transform,
            '', '', // highres_fixed, highres_fixed_dataset,
            '', '', // highres_moving, highres_moving_dataset,
            '', // highres_steps
            '', // highres_output
            '', // highres_transform_name,
            '', // highres_aligned_name,
            '', // scheduler_ip 
            '', // scheduler_work_dir
        ]
    }
    | BIGSTREAM

    emit:
    done
}
