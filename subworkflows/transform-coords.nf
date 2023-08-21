include {
    START_CLUSTER;
} from './start-cluster'

include {
    STOP_CLUSTER;
} from './stop-cluster'

include {
    TRANSFORM_COORDS as TRANSFORM_COORDS_MODULE;
} from '../modules/local/transform-coords'

include {
    normalized_file_name;
    index_channel
} from '../lib/utils'

workflow TRANSFORM_COORDS {
    take:
    input // [coords_file,
          //  transformed_coords_file,
          //  pixel_resolution,
          //  downsampling_factors,
          //  coords_volume,
          //  coords_dataset ]
    transforms // [affine,
               //  vector_field_deform,
               //  vector_field_deform_dataset]

    main:
    def indexed_coords_input = index_channel(input)
    | map {
        def (index, ci) = it
        def (coords_file,
             warped_coords_file,
             pixel_resolution,
             downsampling_factors,
             coords_volume,
             coords_dataset) = ci
        def r = [
            index,
            normalized_file_name(coords_file),
            normalized_file_name(warped_coords_file),
            pixel_resolution,
            downsampling_factors,
            normalized_file_name(coords_volume),
            coords_dataset
        ]
        log.debug "Indexed coords to transform $it -> $r"
        r
    }
    def indexed_transforms = index_channel(transforms)
    | map {
        def (index, ti) = it
        def (affine,
             vector_field_deform,
             vector_field_deform_dataset) = ti
        def r = [
            index,
            normalized_file_name(affine),
            normalized_file_name(vector_field_deform),
            vector_field_deform_dataset,
        ]
        log.debug "Indexed transforms to use $it -> $r"
        r
    }

    def cluster_info = ['', '']

    def transform_coords_results = TRANSFORM_COORDS_MODULE(
        indexed_coords_input.map { it[1..-1] },
        indexed_transforms.map { it[1..-1] },
        cluster_info,
        params.warp_coords_cpus,
        params.warp_coords_mem_gb,
    )

    transform_coords_results.subscribe { log.debug "Completed coords transformation: $it" }

    emit:
    done = transform_coords_results
}
