include {
    GLOBAL_BIGSTREAM_ALIGN;
} from './global-bigstream-align'

include {
    LOCAL_BIGSTREAM_ALIGN;
} from './local-bigstream-align'

include {
    START_CLUSTER;
} from './start_cluster'

include {
    STOP_CLUSTER;
} from './stop_cluster'

include {
    LOCAL_TRANSFORM
} from '../modules/local/local_transform'

include {
    normalized_file_name;
} from '../lib/utils'

workflow BIGSTREAM_REGISTRATION {
    take:
    registration_input // [global_fixed, global_fixed_dataset,
                       //  global_moving, global_moving_dataset,
                       //  global_steps,
                       //  global_output,
                       //  global_transform_name,
                       //  global_aligned_name,
                       //  local_fixed, local_fixed_dataset,
                       //  local_moving, local_moving_dataset,
                       //  local_steps,
                       //  local_output,
                       //  local_transform_name,
                       //  local_aligned_name]
    deform_input       // [[volume_path, volume_dataset, output_warped_name],...] - the list of additional volumes that must be warped

    main:

    def normalized_inputs = registration_input
    | merge(deform_input) // the registration and the deform input are expected to be in sync at this point
    | map {
        def (global_fixed, global_fixed_dataset,
             global_moving, global_moving_dataset,
             global_steps,
             global_output,
             global_transform_name,
             global_aligned_name,
             local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_steps,
             local_output,
             local_transform_name,
             local_aligned_name,
             deform_inputs) = it
        def normalized_local_output = normalized_file_name(local_output)

        def normalized_deform_inputs
        if (deform_inputs != null) {
            normalized_deform_inputs = deform_inputs.collect { vol_inputs ->
                def (vol_path, vol_subpath, vol_output) = vol_inputs
                if (vol_subpath == null) {
                    vol_subpath = ''
                }
                def normalized_vol_output
                if (!vol_output) {
                    normalized_vol_output = "${normalized_local_output}/${file(vol_path).name}"
                } else {
                    normalized_vol_output = normalized_file_name(vol_output)
                }
                [
                    normalized_file_name(vol_path),
                    vol_subpath,
                    normalized_vol_output
                ]
            }
        } else normalized_deform_inputs = []

        [
            normalized_file_name(global_fixed), global_fixed_dataset,
            normalized_file_name(global_moving), global_moving_dataset,
            global_steps,
            normalized_file_name(global_output),
            global_transform_name,
            global_aligned_name,
            normalized_file_name(local_fixed), local_fixed_dataset,
            normalized_file_name(local_moving), local_moving_dataset,
            local_steps,
            normalized_local_output,
            local_transform_name,
            local_aligned_name,
            normalized_deform_inputs
        ]
    }

    def global_inputs = normalized_inputs
    | map {
        def (global_fixed, global_fixed_dataset,
             global_moving, global_moving_dataset,
             global_steps,
             global_output,
             global_transform_name,
             global_aligned_name,
             local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_steps,
             local_output,
             local_transform_name,
             local_aligned_name) = it
        def r = [
            global_fixed, global_fixed_dataset,
            global_moving, global_moving_dataset,
            global_steps,
            global_output,
            global_transform_name,
            global_aligned_name,
        ]
        log.debug "Prepare global alignment: $it -> $r"
        return r
    }

    def global_alignment_results = GLOBAL_BIGSTREAM_ALIGN(global_inputs)

    global_alignment_results.subscribe {
        log.debug "Completed global alignment for: $it"
    }

    // start a dask cluster for local alignment
    def cluster_info = normalized_inputs
    | map {
        def (global_fixed, global_fixed_dataset,
             global_moving, global_moving_dataset,
             global_steps,
             global_output,
             global_transform_name,
             global_aligned_name,
             local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_steps,
             local_output,
             local_transform_name,
             local_aligned_name) = it
        def r = [
            local_fixed,
            local_moving,
            global_output,
            local_output,
            normalized_file_name(params.local_working_path),
        ]
        log.debug "Prepare cluster mounted paths: $it -> $r"
        return r
    }
    | START_CLUSTER
    | map {
        def (cluster_id, cluster_scheduler_ip, cluster_work_dir, connected_workers) = it
        [
            cluster_scheduler_ip, cluster_work_dir,
        ]
    }

    def local_inputs = normalized_inputs
    | join(global_alignment_results, by:[0,1,2,3])
    | map {
        def (global_fixed, global_fixed_dataset,
             global_moving, global_moving_dataset,
             global_steps,
             global_output,
             global_transform_name,
             global_aligned_name,
             local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_steps,
             local_output,
             local_transform_name,
             local_aligned_name) = it
        def r = [
            local_fixed, local_fixed_dataset,
            local_moving, local_moving_dataset,
            local_steps,
            local_output,
            local_transform_name,
            local_aligned_name,
            global_output,
            global_transform_name,
        ]
        log.debug "Prepare local alignment: $it -> $r"
        return r
    }

    def local_alignment_results = LOCAL_BIGSTREAM_ALIGN(local_inputs, cluster_info)

    local_alignment_results[0].subscribe {
        log.debug "Completed local alignment for: $it"
    }

    def local_deform_inputs = normalized_inputs
    | join(local_alignment_results[0], by:[0]) // !!!! FIXME
    | map {
        // FIXME !!!!!!
        it
    }

    // apply local deformation
    def local_deform_results = LOCAL_TRANSFORM(local_deform_inputs, 
                                               local_alignment_results[1],
                                               params.deform_local_cpus,
                                               params.deform_local_mem_gb)

    // done with the cluster
    STOP_CLUSTER(local_deform_results[1].map { it[1] })

    emit:
    done = local_deform_results[0]
}