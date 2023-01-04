import argparse
import numpy as np
import nrrd
import bigstream.n5_utils as n5_utils

from os.path import exists
from ClusterWrap.clusters import (local_cluster, remote_cluster)
from bigstream.align import alignment_pipeline
from bigstream.transform import apply_transform
from bigstream.distributed_align import distributed_alignment_pipeline
from bigstream.piecewise_align import distributed_piecewise_alignment_pipeline
from bigstream.piecewise_transform import distributed_apply_transform


def _inttuple(arg):
    if arg is not None and arg.strip():
        return tuple([int(d) for d in arg.split(',')])
    else:
        return ()


def _floattuple(arg):
    if arg is not None and arg.strip():
        return tuple([float(d) for d in arg.split(',')])
    else:
        return ()


def _stringlist(arg):
    if arg is not None and arg.strip():
        return list(filter(lambda x: x, [s.strip() for s in arg.split(',')]))
    else:
        return []


def _intlist(arg):
    if arg is not None and arg.strip():
        return [int(d) for d in arg.split(',')]
    else:
        return []


class _ArgsHelper:

    def __init__(self, prefix):
        self._prefix = prefix

    def _argflag(self, argname):
        return '--{}-{}'.format(self._prefix, argname)

    def _argdest(self, argname):
        return '{}_{}'.format(self._prefix, argname)


def _define_args(global_descriptor, local_descriptor):
    args_parser = argparse.ArgumentParser(description='Registration pipeline')
    args_parser.add_argument('--fixed-lowres', dest='fixed_lowres',
                             required=True,
                             help='Path to the fixed low resolution volume')
    args_parser.add_argument('--fixed-lowres-subpath',
                             dest='fixed_lowres_subpath',
                             help='Fixed low resolution subpath')

    args_parser.add_argument('--moving-lowres', dest='moving_lowres',
                             required=True,
                             help='Path to the moving low resolution volume')
    args_parser.add_argument('--moving-lowres-subpath',
                             dest='moving_lowres_subpath',
                             help='Moving low resolution subpath')

    args_parser.add_argument('--fixed-highres', dest='fixed_highres',
                             help='Path to the fixed high resolution volume')
    args_parser.add_argument('--fixed-highres-subpath',
                             dest='fixed_highres_subpath',
                             help='Fixed high resolution subpath')

    args_parser.add_argument('--moving-highres', dest='moving_highres',
                             help='Path to the moving high resolution volume')
    args_parser.add_argument('--moving-highres-subpath',
                             dest='moving_highres_subpath',
                             help='Moving high resolution subpath')

    args_parser.add_argument('--use-existing-global-transform',
                             dest='use_existing_global_transform',
                             action='store_true',
                             help='If set use an existing global transform')

    args_parser.add_argument('--output-dir',
                             dest='output_dir',
                             required=True,
                             help='Output directory')
    args_parser.add_argument('--global-registration-steps',
                             dest='global_registration_steps',
                             type=_stringlist,
                             help='Global (lowres) registration steps, e.g. ransac,affine')
    args_parser.add_argument('--global-transform-name',
                             dest='global_transform_name',
                             default='affine-transform.mat',
                             type=str,
                             help='Global transform name')
    args_parser.add_argument('--global-aligned-name',
                             dest='global_aligned_name',
                             type=str,
                             help='Global aligned name')

    _define_ransac_args(args_parser.add_argument_group(
        description='Global ransac arguments'),
        global_descriptor)
    _define_affine_args(args_parser.add_argument_group(
        description='Global affine arguments'),
        global_descriptor)

    args_parser.add_argument('--local-registration-steps',
                             dest='local_registration_steps',
                             type=_stringlist,
                             help='Local (highres) registration steps, .e.g. ransac,deform')
    args_parser.add_argument('--partition-blocksize',
                             dest='partition_blocksize',
                             default=128,
                             type=int,
                             help='blocksize for splitting the work')
    args_parser.add_argument('--local-transform-name',
                             dest='local_transform_name',
                             default='deform-transform',
                             type=str,
                             help='Local transform name')
    args_parser.add_argument('--local-aligned-name',
                             dest='local_aligned_name',
                             type=str,
                             help='Local aligned name')

    _define_ransac_args(args_parser.add_argument_group(
        description='Local ransac arguments'),
        local_descriptor)
    _define_deform_args(args_parser.add_argument_group(
        description='Local deform arguments'),
        local_descriptor)

    args_parser.add_argument('--dask-scheduler', dest='dask_scheduler',
                             type=str, default=None,
                             help='Run with distributed scheduler')

    args_parser.add_argument('--debug', dest='debug',
                             action='store_true',
                             help='Save some intermediate images for debugging')

    return args_parser


def _define_ransac_args(ransac_args, args):
    ransac_args.add_argument(args._argflag('ransac-blob-sizes'),
                             dest=args._argdest('blob_sizes'),
                             metavar='s1,s2,...,sn',
                             type=_intlist,
                             default=[6, 20],
                             help='Ransac blob sizes')


def _define_affine_args(affine_args, args):
    affine_args.add_argument(args._argflag('shrink-factors'),
                             dest=args._argdest('shrink_factors'),
                             metavar='sf1,...,sfn',
                             type=_inttuple, default=None,
                             help='Shrink factors')
    affine_args.add_argument(args._argflag('smooth-sigmas'),
                             dest=args._argdest('smooth_sigmas'),
                             metavar='s1,...,sn',
                             type=_floattuple,
                             help='Smoothing sigmas')
    affine_args.add_argument(args._argflag('learning-rate'),
                             dest=args._argdest('learning_rate'),
                             type=float, default=0.25,
                             help='Learning rate')
    affine_args.add_argument(args._argflag('min-step'),
                             dest=args._argdest('min_step'),
                             type=float, default=0.,
                             help='Minimum step')
    affine_args.add_argument(args._argflag('iterations'),
                             dest=args._argdest('iterations'),
                             type=int, default=100,
                             help='Number of iterations')


def _define_deform_args(deform_args, args):
    deform_args.add_argument(args._argflag('control-point-spacing'),
                             dest=args._argdest('control_point_spacing'),
                             type=float, default=50.,
                             help='Control point spacing')
    deform_args.add_argument(args._argflag('control-point-levels'),
                             dest=args._argdest('control_point_levels'),
                             metavar='s1,...,sn',
                             type=_inttuple, default=(1,),
                             help='Control point levels')
    # deform args are actually a superset of affine args
    _define_affine_args(deform_args, args)


def _check_attr(args, argdescriptor, argname):
    attr_value = getattr(args, argdescriptor._argdest(argname), None)
    return attr_value is not None


def _extract_ransac_args(args, argdescriptor):
    ransac_args = {}
    if _check_attr(args, argdescriptor, 'blob_sizes'):
        ransac_args['blob_sizes'] = getattr(
            args, argdescriptor._argdest('blob_sizes'))
    return ransac_args


def _extract_affine_args(args, argdescriptor):
    affine_args = {'optimizer_args': {}}
    _extract_affine_args_to(args, argdescriptor, affine_args)
    return affine_args


def _extract_affine_args_to(args, argdescriptor, affine_args):
    if _check_attr(args, argdescriptor, 'shrink_factors'):
        affine_args['shrink_factors'] = getattr(
            args, argdescriptor._argdest('shrink_factors'))
    if _check_attr(args, argdescriptor, 'smooth_sigmas'):
        affine_args['smooth_sigmas'] = getattr(
            args, argdescriptor._argdest('smooth_sigmas'))
    if _check_attr(args, argdescriptor, 'learning_rate'):
        affine_args['optimizer_args']['learningRate'] = getattr(
            args, argdescriptor._argdest('learning_rate'))
    if _check_attr(args, argdescriptor, 'min_step'):
        affine_args['optimizer_args']['minStep'] = getattr(
            args, argdescriptor._argdest('min_step'))
    if _check_attr(args, argdescriptor, 'iterations'):
        affine_args['optimizer_args']['numberOfIterations'] = getattr(
            args, argdescriptor._argdest('iterations'))


def _extract_deform_args(args, argdescriptor):
    deform_args = {'optimizer_args': {}}
    _extract_affine_args_to(args, argdescriptor, deform_args)
    if _check_attr(args, argdescriptor, 'control_point_spacing'):
        deform_args['control_point_spacing'] = getattr(
            args, argdescriptor._argdest('control_point_spacing'))
    if _check_attr(args, argdescriptor, 'control_point_levels'):
        deform_args['control_point_levels'] = getattr(
            args, argdescriptor._argdest('control_point_levels'))
    return deform_args


def _run_global_registration(args, steps):
    if args.global_transform_name:
        lowres_transform_file = (args.output_dir + '/' + 
                                 args.global_transform_name)
    else:
        lowres_transform_file = None

    if (args.use_existing_global_transform and
        lowres_transform_file and
            exists(lowres_transform_file)):
        print('Read global transform from', lowres_transform_file, flush=True)
        lowres_transform = np.loadtxt(lowres_transform_file)
    elif steps:
        print('Run global registration with:', args, steps, flush=True)
        # Read the the lowres inputs
        fix_lowres_ldata, fix_lowres_attrs = n5_utils.open(
            args.fixed_lowres, args.fixed_lowres_subpath)
        mov_lowres_ldata, mov_lowres_attrs = n5_utils.open(
            args.moving_lowres, args.moving_lowres_subpath)
        fix_lowres_voxel_spacing = n5_utils.get_voxel_spacing(fix_lowres_attrs)
        mov_lowres_voxel_spacing = n5_utils.get_voxel_spacing(mov_lowres_attrs)

        print('Fixed lowres volume attributes:',
              fix_lowres_ldata.shape, fix_lowres_voxel_spacing, flush=True)
        print('Moving lowres volume attributes:',
              mov_lowres_ldata.shape, mov_lowres_voxel_spacing, flush=True)

        lowres_transform, lowres_alignment = _run_lowres_alignment(
            fix_lowres_ldata[...],  # read image in memory
            mov_lowres_ldata[...],
            fix_lowres_voxel_spacing,
            mov_lowres_voxel_spacing,
            steps)

        if lowres_transform_file:
            # save the lowres transformation
            np.savetxt(lowres_transform_file, lowres_transform)

        if args.global_aligned_name:
            lowres_aligned_file = (args.output_dir + '/' + 
                                   args.global_aligned_name)
            # for now save it as nrrd
            nrrd.write(lowres_aligned_file,
                       lowres_alignment.transpose(2, 1, 0),
                       compression_level=2)
    else:
        print('Skip global alignment because no global steps were specified.')
        lowres_transform = None

    return lowres_transform


def _run_lowres_alignment(fix_data,
                          mov_data,
                          fix_spacing,
                          mov_spacing,
                          steps):
    print('Run low res alignment:', steps, flush=True)
    affine = alignment_pipeline(fix_data,
                                mov_data,
                                fix_spacing,
                                mov_spacing,
                                steps)
    print('Apply affine transform', flush=True)
    # apply transform
    aligned = apply_transform(fix_data,
                              mov_data,
                              fix_spacing,
                              mov_spacing,
                              transform_list=[affine,])

    return affine, aligned


def _run_local_registration(args, steps, global_transform):
    if steps:
        print('Run local registration with:', steps, flush=True)

        # Read the highres inputs - if highres is not defined default it to lowres
        fix_highres_path = args.fixed_highres if args.fixed_highres else args.fixed_lowres
        mov_highres_path = args.fixed_highres if args.fixed_highres else args.fixed_lowres

        fix_highres_ldata, fix_highres_attrs = n5_utils.open(
            fix_highres_path, args.fixed_highres_subpath)
        mov_highres_ldata, mov_highres_attrs = n5_utils.open(
            mov_highres_path, args.moving_highres_subpath)
        fix_highres_voxel_spacing = n5_utils.get_voxel_spacing(
            fix_highres_attrs)
        mov_highres_voxel_spacing = n5_utils.get_voxel_spacing(
            mov_highres_attrs)

        print('Fixed highres volume attributes:',
              fix_highres_ldata.shape, fix_highres_voxel_spacing, flush=True)
        print('Moving highres volume attributes:',
              mov_highres_ldata.shape, mov_highres_voxel_spacing, flush=True)

        if args.dask_scheduler:
            cluster = remote_cluster(args.dask_scheduler)
        else:
            cluster = local_cluster()

        _run_highres_alignment(
            fix_highres_ldata,
            mov_highres_ldata,
            fix_highres_voxel_spacing,
            mov_highres_voxel_spacing,
            steps,
            args.partition_blocksize,
            [global_transform] if global_transform is not None else [],
            args.output_dir,
            args.local_transform_name,
            args.local_aligned_name,
            cluster
        )
    else:
        print('Skip local alignment because no local steps were specified.')


def _run_highres_alignment(fix_data,
                           mov_data,
                           fix_spacing,
                           mov_spacing,
                           steps,
                           partitionsize,
                           transforms_list,
                           output_dir,
                           highres_transform_name,
                           highres_aligned_name,
                           cluster):
    print('Run high res alignment:', steps, partitionsize, flush=True)
    if highres_transform_name:
        deform_transform_output = output_dir + '/' + highres_transform_name
    else:
        deform_transform_output = None
    deform = distributed_alignment_pipeline(
        fix_data, mov_data,
        fix_spacing, mov_spacing,
        steps,
        partitionsize,
        static_transform_list=transforms_list,
        write_path=deform_transform_output,
        cluster=cluster,
    )

    aligned = distributed_apply_transform(
        fix_data, mov_data,
        fix_spacing, mov_spacing,
        transform_list=transforms_list + [deform],
        blocksize=partitionsize,
        write_path=output_dir if highres_aligned_name else None,
        dataset_path=highres_aligned_name,
        cluster=cluster,
    )
    return deform, aligned


if __name__ == '__main__':
    global_descriptor = _ArgsHelper('global')
    local_descriptor = _ArgsHelper('local')
    args_parser = _define_args(global_descriptor, local_descriptor)
    args = args_parser.parse_args()
    print('Invoked registration:', args, flush=True)

    if args.global_registration_steps:
        args_for_global_steps = {
            'ransac': _extract_ransac_args(args, global_descriptor),
            'affine': _extract_affine_args(args, global_descriptor),
        }
        global_steps = [(s, args_for_global_steps.get(s, {}))
                        for s in args.global_registration_steps]
    else:
        global_steps = []

    global_transform = _run_global_registration(args, global_steps)

    if args.local_registration_steps:
        args_for_local_steps = {
            'ransac': _extract_ransac_args(args, local_descriptor),
            'deform': _extract_deform_args(args, local_descriptor),
        }
        local_steps = [(s, args_for_local_steps.get(s, {}))
                       for s in args.local_registration_steps]
    else:
        local_steps = []

    _run_local_registration(args, local_steps, global_transform)
