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
from bigstream.distributed_transform import distributed_apply_transform


def _stringlist(arg):
    if arg is not None and arg.strip():
        return list(filter(lambda x: x, [s.strip() for s in arg.split(',')]))
    else:
        return []


def _define_args():
    args_parser = argparse.ArgumentParser(description='Apply transformation')
    args_parser.add_argument('--fixed', dest='fixed',
                             help='Path to the fixed image')
    args_parser.add_argument('--fixed-subpath',
                             dest='fixed_subpath',
                             help='Fixed image subpath')

    args_parser.add_argument('--moving', dest='moving',
                             help='Path to the moving image')
    args_parser.add_argument('--moving-subpath',
                             dest='moving_subpath',
                             help='Moving image subpath')

    args_parser.add_argument('--global-transformations',
                             dest='global_transformations',
                             type=_stringlist,
                             help='Global transformations')

    args_parser.add_argument('--local-transform', dest='local_transform',
                             help='Local transformation')
    args_parser.add_argument('--local-transform-subpath',
                             dest='local_transform_subpath',
                             help='Transformation to be applied')

    args_parser.add_argument('--output-dir',
                             dest='output_dir',
                             help='Output directory')
    args_parser.add_argument('--working-dir',
                             dest='working_dir',
                             help='Working directory')
    args_parser.add_argument('--warped-name',
                             dest='warped_name',
                             help='Name of the deformed image')
    args_parser.add_argument('--warped-subpath',
                             dest='warped_subpath',
                             help='Subpath for the warped output')
    args_parser.add_argument('--output-chunk-size',
                             dest='output_chunk_size',
                             default=128,
                             type=int,
                             help='Output chunk size')

    args_parser.add_argument('--partition-blocksize',
                             dest='partition_blocksize',
                             default=128,
                             type=int,
                             help='blocksize for splitting the work')

    args_parser.add_argument('--dask-scheduler', dest='dask_scheduler',
                             type=str, default=None,
                             help='Run with distributed scheduler')

    args_parser.add_argument('--dask-config', dest='dask_config',
                             type=str, default=None,
                             help='YAML file containing dask configuration')

    return args_parser


def _run_apply_transform(args):

    # Read the highres inputs - if highres is not defined default it to lowres
    fix_subpath = args.fixed_subpath
    mov_subpath = args.moving_subpath if args.moving_subpath else fix_subpath
    warped_subpath = args.warped_subpath if args.warped_subpath else mov_subpath

    fix_data, fix_attrs = n5_utils.open(args.fixed, fix_subpath)
    mov_data, mov_attrs = n5_utils.open(args.moving, mov_subpath)
    fix_voxel_spacing = n5_utils.get_voxel_spacing(fix_attrs)
    mov_voxel_spacing = n5_utils.get_voxel_spacing(mov_attrs)

    print('Fixed volume attributes:',
          fix_data.shape, fix_voxel_spacing, flush=True)
    print('Moving volume attributes:',
          mov_data.shape, mov_voxel_spacing, flush=True)

    if (args.dask_config):
        with open(args.dask_config) as f:
            dask_config = flatten(yaml.safe_load(f))
    else:
        dask_config = {}

    if args.dask_scheduler:
        cluster = remote_cluster(args.dask_scheduler, config=dask_config)
    else:
        cluster = local_cluster(config=dask_config)

    local_deform = n5_utils.open(args.local_trasform, 
                                 args.local_transform_subpath)

    output_blocks = (args.output_chunk_size,) * fix_data.ndim

    if args.output_dir and args.warped_name:
        warped_dataset = n5_utils.create_dataset(
            args.output_dir + '/' + args.warped_name,
            warped_subpath,
            fix_data.shape,
            output_blocks,
            fix_data.dtype,
        )

        if args.global_transformations:
            transforms_list = [np.loadtxt(tfile) 
                               for tfile in args.global_transformations]
        else:
            transforms_list = []            

        warped = distributed_apply_transform(
            fix_data, mov_data,
            fix_voxel_spacing, mov_voxel_spacing,
            args.partition_blocksize,
            output_blocks,
            transform_list=transforms_list + [local_deform],
            aligned_dataset=warped_dataset,
            cluster=cluster,
            temporary_directory=args.working_dir,
        )
        return warped
    else:
        return None


if __name__ == '__main__':
    args_parser = _define_args()
    args = args_parser.parse_args()
    print('Invoked transformation:', args, flush=True)

    _run_apply_transform(args)