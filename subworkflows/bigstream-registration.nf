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
    index_channel
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

    def indexed_registration_input = index_channel(registration_input)
    | map {
        def (index, ri) = it
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
             local_aligned_name) = ri
        [
            index,
            global_fixed, global_fixed_dataset,
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
        ]
    }
    def indexed_deform_input = index_channel(deform_input)

    def normalized_inputs = indexed_registration_input
    | join(indexed_deform_input, by:0) // the registration and the deform input are expected to be in sync at this point
    | map {
        def (index,
             global_fixed, global_fixed_dataset,
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

        log.debug "Normalize: $it"

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
                def r = [
                    normalized_file_name(vol_path),
                    vol_subpath,
                    normalized_vol_output
                ]
                log.debug "Normalize deform inputs: $vol_inputs -> $r"
                r
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

    // start a dask cluster for local alignment
    def cluster_input = local_inputs
    | map {
        def (local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_steps,
             local_output,
             local_transform_name,
             local_aligned_name,
             global_output,
             global_transform_name) = it
        def r = [
            [
                local_fixed, local_moving,
            ],
            [
                global_output,
                local_output,
                normalized_file_name(params.local_working_path),
            ],
        ]
        log.debug "Prepare cluster mounted paths: $it -> $r"
        return r
    }

    def cluster_info = START_CLUSTER(cluster_input.map { it[0] },
                                     cluster_input.map { it[1] })
    | map {
        def (cluster_id, cluster_scheduler, cluster_workdir, connected_workers) = it
        def current_cluster_info = [
            cluster_scheduler, cluster_workdir,
        ]
        log.debug "Current cluster: $it -> ${current_cluster_info}"
        current_cluster_info
    }


    def local_alignment_results = LOCAL_BIGSTREAM_ALIGN(local_inputs, cluster_info)

    local_alignment_results.subscribe {
        log.debug "Completed local alignment for: $it"
    }

    def local_deform_inputs = normalized_inputs
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
        [
             local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_output,
             local_transform_name,
             local_aligned_name,
             global_output,
             global_transform_name,
             deform_inputs
        ]
    }
    | join(local_alignment_results, by:[0,1,2,3,4,5,6,7,8])
    | flatMap {
        def (local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_output,
             local_transform_name,
             local_aligned_name,
             global_output,
             global_transform_name,
             deform_inputs,
             cluster_scheduler,
             cluster_workdir) = it

        log.debug "Build deform inputs from: $it"

        deform_inputs.collect { vol_inputs ->
            def (vol_path, vol_subpath, vol_output) = vol_inputs
            def r = [
                local_fixed, local_fixed_dataset,
                vol_path, vol_subpath,
                vol_output, vol_subpath,
                "${global_output}/${global_transform_name}",
                "${local_output}/${local_transform_name}", ''/* empty_local_transform_subpath */,
            ]
            def current_cluster_info = [cluster_scheduler, cluster_workdir]
            log.debug "Local transform inputs: $r, $current_cluster_info"
            [r, current_cluster_info]
        }
    }

    // apply local deformation
    def local_deform_results = LOCAL_TRANSFORM(local_deform_inputs.map { it[0] }, 
                                               local_deform_inputs.map { it[1] },
                                               params.deform_local_cpus,
                                               params.deform_local_mem_gb)

    local_deform_results.subscribe { log.debug "Completed deform transformation: $it" }

    // done with the cluster
    def deform_results_by_cluster = local_deform_results
    | groupTuple(by: [0,1,6,7]) // group all processes that run on the same cluster 
                                // and also include the fixed image since it is the same 
                                // based on how the input was created
    | map {
        def (fixed_path, fixed_subpath,
             moving_path, moving_subpath,
             output_path, output_subpath,
             cluster_scheduler, cluster_workdir) = it
        def deformed_results = [
            moving_path, moving_subpath,
            output_path, output_subpath,
        ]
        def r = [
            fixed_path, fixed_subpath,
            deformed_results.transpose(),
            cluster_scheduler, cluster_workdir,
        ]
        log.debug "Prepare to gather final results $it -> $r (${r[4]})"
        r
    }
    
    STOP_CLUSTER(deform_results_by_cluster.map { it[4] })

    def final_results = normalized_inputs
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
        [
             local_fixed, local_fixed_dataset,
             local_moving, local_moving_dataset,
             local_output,
             local_transform_name,
             local_aligned_name,
             global_fixed, global_fixed_dataset,
             global_moving, global_moving_dataset,
             global_output,
             global_transform_name,
             global_aligned_name,
        ]
    }
    | join(deform_results_by_cluster, by:[0,1])
    | map {
        def(
            local_fixed, local_fixed_dataset,
            local_moving, local_moving_dataset,
            local_output,
            local_transform_name,
            local_aligned_name,
            global_fixed, global_fixed_dataset,
            global_moving, global_moving_dataset,
            global_output,
            global_transform_name,
            global_aligned_name,
            deformed_results
        ) = it
        def r = [
            global_fixed, global_fixed_dataset,
            global_moving, global_moving_dataset,
            global_output,
            global_transform_name,
            global_aligned_name,
            local_fixed, local_fixed_dataset,
            local_moving, local_moving_dataset,
            local_output,
            local_transform_name,
            local_aligned_name,
            deformed_results,
        ]
        log.debug "Final results $it -> $r"
        r
    }

    emit:
    done = final_results
}
