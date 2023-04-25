include {
    get_runtime_opts;
    parentfile;
    normalized_file_name;
} from '../../lib/utils'

process TRANSFORM_COORDS {
    container { params.bigstream_container }
    containerOptions { get_runtime_opts([
        parentfile(input_coords, 1),
        parentfile(warped_coords, 2),
        parentfile(input_coords_volume, 1),
        parentfile(affine_transform, 1),
        parentfile(vector_field_transform_path, 1),
        parentfile(params.local_working_path, 2),
        parentfile(params.dask_config, 1)]) }

    memory { "${mem_gb} GB" }
    cpus { ncpus }

    input:
    tuple val(input_coords),
          val(warped_coords),
          val(pixel_resolution),
          val(downsampling_factors),
          val(input_coords_volume),
          val(input_coords_dataset)
    tuple val(affine_transform),
          val(vector_field_transform_path),
          val(vector_field_transform_subpath)
    tuple val(cluster_scheduler),
          val(cluster_workdir)
    val(ncpus)
    val(mem_gb)

    output:
    tuple val(input_coords), val(warped_coords),
          val(cluster_scheduler), val(cluster_workdir)

    script:
    def warped_coords_dir = file(warped_coords).parent

    def pixel_resolutions_arg = pixel_resolution
        ? "--pixel-resolution ${pixel_resolution}"
        : ''
    def downsampling_factors_arg = downsampling_factors
        ? "--downsampling ${downsampling_factors}"
        : ''
    def input_coords_volume_arg = input_coords_volume
        ? "--input-volume ${input_coords_volume}"
        : ''
    def input_coords_dataset_arg = input_coords_dataset
        ? "--input-dataset ${input_coords_dataset}"
        : ''
    def affine_transforms_arg = affine_transform
        ? "--affine-transformations ${affine_transform}"
        : ''
    def vector_field_transform_arg = vector_field_transform_path
        ? "--vector-field-transform ${vector_field_transform_path}"
        : ''
    def vector_field_transform_subpath_arg = vector_field_transform_subpath
        ? "--vector-field-transform-subpath ${vector_field_transform_subpath}"
        : ''
    def coords_partitionsize_arg = params.warp_coords_partitionsize > 0
        ? "--partition-size ${params.warp_coords_partitionsize}"
        : ''

    def mk_working_dir = params.local_working_path
        ? "mkdir -p ${normalized_file_name(params.local_working_path)}"
        : ''
    def working_dir_arg = params.local_working_path
        ? "--working-dir ${normalized_file_name(params.local_working_path)}"
        : ''
    def scheduler_arg = cluster_scheduler
        ? "--dask-scheduler ${cluster_scheduler}"
        : ''
    def dask_config_arg = params.dask_config
        ? "--dask-config ${normalized_file_name(params.dask_config)}"
        : ''
    """
    umask 0002
    mkdir -p ${warped_coords_dir}
    ${mk_working_dir}
    python /app/bigstream/scripts/main_apply_transform_coords.py \
        --input-coords ${input_coords} \
        --output-coords ${warped_coords} \
        ${pixel_resolutions_arg} \
        ${downsampling_factors_arg} \
        ${input_coords_volume_arg} \
        ${input_coords_dataset_arg} \
        ${affine_transforms_arg} \
        ${vector_field_transform_arg} \
        ${vector_field_transform_subpath_arg} \
        ${working_dir_arg} \
        ${coords_partitionsize_arg} \
        ${scheduler_arg} \
        ${dask_config_arg}
    """
}
