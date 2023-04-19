include {
    BIGSTREAM;
} from '../modules/bigstream/main'

include {
    normalized_file_name;
} from '../lib/utils'

// This workflow aligns a chunked volume, block by block
workflow LOCAL_BIGSTREAM_ALIGN {
    take:
    align_input // [local_fixed, local_fixed_dataset,
                //  local_moving, local_moving_dataset,
                //  local_steps,
                //  local_output,
                //  local_transform_name,
                //  local_aligned_name
                //  global_transform_dir,
                //  global_transform_name]
    cluster_info

    main:
    def bigstream_input = align_input
    | map {
        def (local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_steps,
             local_output,
             local_transform_name,
             local_aligned_name,
             global_transform_dir,
             global_transform_name) = it
        [
            '', '', // global_fixed, global_fixed_dataset
            '', '', // global_moving, global_moving_dataset
            '', // global_steps,
            normalized_file_name(global_transform_dir),
            global_transform_name,
            '', // global_aligned_name
            normalized_file_name(local_fixed), local_fixed_dataset,
            normalized_file_name(local_moving), local_moving_dataset,
            local_steps,
            normalized_file_name(local_output),
            local_transform_name,
            local_aligned_name,
        ]
    }

    def bigstream_results = BIGSTREAM(bigstream_input,
                                      true, // use_existing_global_transform
                                      params.bigstream_local_cpus,
                                      params.bigstream_local_mem_gb,
                                      cluster_info)

    def local_alignment_results = bigstream_results[0]
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
             local_aligned_name) = it
        [
            local_fixed, local_fixed_dataset,
            local_moving, local_moving_dataset,
            local_output,
            local_transform_name,
            local_aligned_name,
            global_output,
            global_transform_name,
        ]
    }

    emit:
    results = local_alignment_results
    bigstream_results[1]
}
