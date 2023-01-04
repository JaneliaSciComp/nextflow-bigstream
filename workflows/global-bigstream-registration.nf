include {
    start_cluster;
} from '../subworkflows/start_cluster'

include {
    stop_cluster;
} from '../subworkflows/stop_cluster'

include {
    BIGSTREAM;
} from '../modules/bigstream/main'

workflow GLOBAL_BIGSTREAM_REGISTRATION {
    take:
    registration_input // [fixed_lowres, fixed_lowres_dataset,
                       //  moving_lowres, moving_lowres_dataset,
                       //  global_steps,
                       //  output, global_transform_name, global_aligned_name]

    main:

    done = registration_input
    | map {
        log.info "Run global registration with: $it"
        def (fixed_lowres, fixed_lowres_dataset,
             moving_lowres, moving_lowres_dataset,
             global_steps,
             output, global_transform_name, global_aligned_name) = it
        [
            fixed_lowres, fixed_lowres_dataset,
            moving_lowres, moving_lowres_dataset,
            '', '', // fixed_highres, fixed_highres_dataset,
            '', '', // moving_highres, moving_highres_dataset,
            output,
            global_steps,
            global_transform_name,
            global_aligned_name,
            '', // local_steps
            '', // local_transform_name,
            '', // local_aligned_name,
            '', // scheduler_ip 
            '', // scheduler_work_dir
        ]
    }
    | BIGSTREAM

    emit:
    done
}
