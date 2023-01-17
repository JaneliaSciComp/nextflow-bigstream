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
    def bigstream_input = align_input
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
            '', '', // highres_fixed, highres_fixed_dataset,
            '', '', // highres_moving, highres_moving_dataset,
            '', // highres_steps
            '', // highres_output
            '', // highres_transform_name,
            '', // highres_aligned_name,
        ]
    }

    def bigstream_results = BIGSTREAM(bigstream_input,
                                      params.use_existing_global_transform,
                                      params.bigstream_lowres_cpus,
                                      params.bigstream_lowres_mem_gb,
                                      [
                                         '', // scheduler_ip
                                         '', // cluster_work_dir
                                      ])

    emit:
    done = bigstream_results[0]
}
