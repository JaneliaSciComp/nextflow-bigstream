include {
    START_CLUSTER;
} from './start_cluster'

include {
    STOP_CLUSTER;
} from './stop_cluster'

include {
    TRANSFORM_COORDS
} from '../modules/local/transform_coords'

include {
    normalized_file_name;
    index_channel
} from '../lib/utils'

workflow TRANSFORM_COORDS {
    take:
    input // [coords_file,
          //  transformed_coords_file,
          //  coords_volume,
          //  coords_dataset ]
    transforms // [affine,
               //  vector_field_deform,
               //  vector_field_deform_dataset]

    main:

    def transform_coords_results = TRANSFORM_COORDS(
        input,
        transforms,
        Channel.of([])
    )

    transform_coords_results.subscribe { log.debug "Completed coords transformation: $it" }

    emit:
    done = transform_coords_results
}
