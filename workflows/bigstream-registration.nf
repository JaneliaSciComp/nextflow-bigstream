include {
    start_cluster;
} from '../subworkflows/start_cluster'

include {
    stop_cluster;
} from '../subworkflows/stop_cluster'

include {
    GLOBAL_BIGSTREAM_ALIGN;
} from './global-bigstream-align'

include {
    LOCAL_BIGSTREAM_ALIGN;
} from './local-bigstream-align'

workflow BIGSTREAM_REGISTRATION {
    take:
    registration_input // [lowres_fixed, lowres_fixed_dataset,
                       //  lowres_moving, lowres_moving_dataset,
                       //  lowres_steps,
                       //  lowres_output,
                       //  lowres_transform_name,
                       //  lowres_aligned_name,
                       //  highres_fixed, highres_fixed_dataset,
                       //  highres_moving, highres_moving_dataset,
                       //  highres_steps,
                       //  highres_output,
                       //  highres_transform_name,
                       //  highres_aligned_name]
    main:

    def normalized_inputs = registration_input
    | map {
        def (lowres_fixed, lowres_fixed_dataset,
             lowres_moving, lowres_moving_dataset,
             lowres_steps,
             lowres_output,
             lowres_transform_name,
             lowres_aligned_name,
             highres_fixed, highres_fixed_dataset,
             highres_moving, highres_moving_dataset,
             highres_steps,
             highres_output,
             highres_transform_name,
             highres_aligned_name) = it
        [
            normalized_file_name(lowres_fixed), lowres_fixed_dataset,
            normalized_file_name(lowres_moving), lowres_moving_dataset,
            lowres_steps,
            normalized_file_name(lowres_output),
            lowres_transform_name,
            lowres_aligned_name,
            normalized_file_name(highres_fixed), highres_fixed_dataset,
            normalized_file_name(highres_moving), highres_moving_dataset,
            highres_steps,
            normalized_file_name(highres_output),
            highres_transform_name,
            highres_aligned_name            
        ]
    }

    def lowres_inputs = normalized_inputs
    | map {
        def (lowres_fixed, lowres_fixed_dataset,
             lowres_moving, lowres_moving_dataset,
             lowres_steps,
             lowres_output,
             lowres_transform_name,
             lowres_aligned_name,
             highres_fixed, highres_fixed_dataset,
             highres_moving, highres_moving_dataset,
             highres_steps,
             highres_output,
             highres_transform_name,
             highres_aligned_name) = it
        def r = [
            lowres_fixed, lowres_fixed_dataset,
            lowres_moving, lowres_moving_dataset,
            lowres_steps,
            lowres_output,
            lowres_transform_name,
            lowres_aligned_name,
        ]
        log.debug "Prepare lowres alignment: $it -> $r"
        return r
    }

    def lowres_alignment_results = GLOBAL_BIGSTREAM_ALIGN(lowres_inputs)

    lowres_alignment_results.subscribe {
        log.debug "Completed lowres alignment for: $it"
    }

    def highres_inputs = normalized_inputs
    | join(lowres_alignment_results, by:[0,1,2,3])
    | map {
        def (lowres_fixed, lowres_fixed_dataset,
             lowres_moving, lowres_moving_dataset,
             lowres_steps,
             lowres_output,
             lowres_transform_name,
             lowres_aligned_name,
             highres_fixed, highres_fixed_dataset,
             highres_moving, highres_moving_dataset,
             highres_steps,
             highres_output,
             highres_transform_name,
             highres_aligned_name) = it
        def r = [
            highres_fixed, highres_fixed_dataset,
            highres_moving, highres_moving_dataset,
            highres_steps,
            highres_output,
            highres_transform_name,
            highres_aligned_name,
            lowres_output,
            lowres_transform_name,
        ]
        log.debug "Prepare highres alignment: $it -> $r"
        return r
    }

    def highres_alignment_results = LOCAL_BIGSTREAM_ALIGN(highres_inputs)

    highres_alignment_results.subscribe {
        log.debug "Completed highres alignment for: $it"
    }

    emit:
    done = highres_alignment_results
}
