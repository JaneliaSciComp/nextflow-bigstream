import argparse
import numpy as np
import nrrd
import bigstream.n5_utils as n5_utils
import yaml

from flatten_json import flatten
from os.path import exists
from ClusterWrap.clusters import (local_cluster, remote_cluster)
from bigstream.align import alignment_pipeline
from bigstream.transform import apply_transform
from bigstream.distributed_align import distributed_alignment_pipeline
from bigstream.distributed_transform import (distributed_apply_transform,
     distributed_apply_transform_to_coordinates)


def _inttuple(arg):
    if arg is not None and arg.strip():
        return tuple([int(d) for d in arg.split(',')])
    else:
        return None


def _floattuple(arg):
    if arg is not None and arg.strip():
        return tuple([float(d) for d in arg.split(',')])
    else:
        return None


def _stringlist(arg):
    if arg is not None and arg.strip():
        return list(filter(lambda x: x, [s.strip() for s in arg.split(',')]))
    else:
        return []


def _define_args():
    args_parser = argparse.ArgumentParser(description='Apply transformation')
    args_parser.add_argument('--input-coords', dest='input_coords',
                             help='Path to input coordinates file')
    args_parser.add_argument('--pixel-resolution',
                             dest='pixel_resolution',
                             type=_floattuple,
                             help='Pixel resolution')
    args_parser.add_argument('--downsampling',
                             dest='downsampling',
                             type=_inttuple,
                             help='Downsampling factors')

    args_parser.add_argument('--output-coords', dest='output_coords',
                             help='Path to warped coordinates file')

    args_parser.add_argument('--affine-transformations',
                             dest='affine_transformations',
                             type=_stringlist,
                             help='Affine transformations')

    args_parser.add_argument('--local-transform', dest='local_transform',
                             help='Local (vector field) transformation')
    args_parser.add_argument('--local-transform-subpath',
                             dest='local_transform_subpath',
                             help='Local transformation dataset to be applied')

    args_parser.add_argument('--working-dir',
                             dest='working_dir',
                             help='Working directory')

    args_parser.add_argument('--partition-size',
                             dest='partition_size',
                             default=30,
                             type=int,
                             help='Partition size for splitting the work')

    args_parser.add_argument('--dask-scheduler', dest='dask_scheduler',
                             type=str, default=None,
                             help='Run with distributed scheduler')

    args_parser.add_argument('--dask-config', dest='dask_config',
                             type=str, default=None,
                             help='YAML file containing dask configuration')

    return args_parser


def _get_voxel_spacing(pixel_resolution, downsampling_factors):
    if (pixel_resolution is not None and
        downsampling_factors is not None):
        voxel_spacing = (np.array(pixel_resolution) * 
                         np.array(downsampling_factors))
    elif (pixel_resolution is not None):
        voxel_spacing = np.array(pixel_resolution)
    # if voxel spacing is set return it in zyx order
    return voxel_spacing[::-1] if voxel_spacing is not None else None


def _run_apply_transform(args):

    if not args.input_coords:
        # Nothing to do
        return

    # Read the input coordinates
    coords = np.float32(np.loadtxt(args.input_coords, delimiter=','))

    if (args.dask_config):
        with open(args.dask_config) as f:
            dask_config = flatten(yaml.safe_load(f))
    else:
        dask_config = {}

    if args.dask_scheduler:
        cluster = remote_cluster(args.dask_scheduler, config=dask_config)
    else:
        cluster = local_cluster(config=dask_config)

    # read local deform, but ignore attributes as they are not needed
    local_deform, _ = n5_utils.open(args.local_transform,
                                    args.local_transform_subpath)

    if args.output_coords:
        if args.affine_transformations:
            affine_transforms_list = [np.loadtxt(tfile)
                                      for tfile in args.affine_transformations]
        else:
            affine_transforms_list = []

        voxel_spacing = _get_voxel_spacing(args.pixel_resolution,
                                           args.downsampling)

        warped_coords = distributed_apply_transform_to_coordinates(
            coords,
            affine_transforms_list + [local_deform],
            partition_size=args.partition_size,
            coords_spacing=voxel_spacing,
            cluster=cluster,
            temporary_directory=args.working_dir,
        )
        np.savetxt(args.output_coords, warped_coords, delimiter=',')
        return args.output_coords
    else:
        return None


if __name__ == '__main__':
    args_parser = _define_args()
    args = args_parser.parse_args()
    print('Invoked transformation:', args, flush=True)

    _run_apply_transform(args)