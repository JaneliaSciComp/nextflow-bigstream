include {
    BIGSTREAM;
} from '../modules/bigstream/main'

include {
    normalized_file_name;
} from '../lib/utils'

// This workflow aligns the entire volume
// with the restriction that the entire volume must fit in memory
// If the volume is large this workflow uses a lower resolution 
// in order to be able to fit the entire volume in memory
workflow GLOBAL_BIGSTREAM_ALIGN {
    take:
    align_input // [global_fixed, global_fixed_dataset,
                //  global_moving, global_moving_dataset,
                //  global_steps,
                //  global_output,
                //  global_transform_name,
                //  global_aligned_name]

    main:
    // global alignment does not require a dask cluster
    def bigstream_input = align_input
    | map {
        log.info "Run global registration with: $it"
        def (global_fixed, global_fixed_dataset,
             global_moving, global_moving_dataset,
             global_steps,
             global_output,
             global_transform_name,
             global_aligned_name) = it
        [
            normalized_file_name(global_fixed), global_fixed_dataset,
            normalized_file_name(global_moving), global_moving_dataset,
            global_steps,
            normalized_file_name(global_output),
            global_transform_name,
            global_aligned_name,
            '', '', // local_fixed, local_fixed_dataset,
            '', '', // local_moving, local_moving_dataset,
            '', // local_steps
            '', // local_output
            '', // local_transform_name,
            '', // local_inv_transform_name
            '', // local_aligned_name,
        ]
    }

    def bigstream_results = BIGSTREAM(bigstream_input,
                                      params.use_existing_global_transform,
                                      params.bigstream_global_cpus,
                                      params.bigstream_global_mem_gb,
                                      [
                                         '', // scheduler_ip
                                         '', // cluster_work_dir
                                      ])

    def global_alignment_results = bigstream_results
    | map {
        def (global_fixed, global_fixed_dataset,
             global_moving, global_moving_dataset,
             global_output,
             global_transform_name,
             global_aligned_name,
             local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_output,
             local_transform_name,
             local_inv_transform_name,
             local_aligned_name) = it
        [
            global_fixed, global_fixed_dataset,
            global_moving, global_moving_dataset,
            global_output,
            global_transform_name,
            global_aligned_name,
        ]
    }

    emit:
    done = global_alignment_results
}
