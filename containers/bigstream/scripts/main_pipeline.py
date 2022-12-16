import argparse
import numpy as np
import nrrd
import bigstream.n5_utils as n5_utils

from ClusterWrap.clusters import (local_cluster, remote_cluster)
from bigstream.align import alignment_pipeline
from bigstream.transform import apply_transform
from bigstream.piecewise_align import distributed_piecewise_alignment_pipeline
from bigstream.piecewise_transform import distributed_apply_transform


def _inttuple(arg):
    return tuple([int(d) for d in arg.split(',')])


def _floattuple(arg):
    return tuple([float(d) for d in arg.split(',')])


def _stringlist(arg):
    return list(filter(lambda x: x, [s.strip() for s in arg.split(',')]))


def _intlist(arg):
    return [int(d) for d in arg.split(',')]


def _define_args():
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

    ransac_args = args_parser.add_argument_group(
        description='Ransac arguments')
    ransac_args.add_argument('--ransac-blob-sizes',
                             metavar='s1,s2,...,sn',
                             dest='ransac_blob_sizes', type=_intlist,
                             default=[6, 20])
    ransac_args.add_argument('--ransac-threshold',
                             dest='ransac_threshold', type=float)
    ransac_args.add_argument('--ransac-confidence', dest='ransac_confidence',
                             type=float)

    affine_args = args_parser.add_argument_group(
        description='Affine arguments')
    affine_args.add_argument('--affine-shrink-factors', dest='affine_shrink_factors',
                             metavar='sf1,...,sfn',
                             type=_inttuple, default=(2,), help='Shrink factors')
    affine_args.add_argument('--affine-smooth-sigmas', dest='affine_smooth_sigmas',
                             metavar='s1,...,sn',
                             type=_floattuple, help='Smoothing sigmas')
    affine_args.add_argument('--affine-learning-rate', dest='affine_learning_rate',
                             type=float, default=0.25, help='Learning rate')
    affine_args.add_argument('--affine-min-step', dest='affine_min_step',
                             type=float, default=0., help='Minimum step')
    affine_args.add_argument('--affine-iterations', dest='affine_iterations',
                             type=int, default=400,
                             help='Number of iterations')

    deform_args = args_parser.add_argument_group(
        description='Deform arguments')
    deform_args.add_argument('--deform-smooth-sigmas', dest='deform_smooth_sigmas',
                             metavar='s1,...,sn',
                             type=_floattuple, help='Smoothing sigmas')
    deform_args.add_argument('--deform-control-point-spacing',
                             dest='deform_control_point_spacing',
                             type=float, default=50.,
                             help='Control point spacing')
    deform_args.add_argument('--deform-control-point-levels',
                             dest='deform_control_point_levels',
                             metavar='s1,...,sn',
                             type=_inttuple, default=(1,),
                             help='Control point levels')
    deform_args.add_argument('--deform-learning-rate', dest='deform_learning_rate',
                             type=float, default=0.25, help='Learning rate')
    deform_args.add_argument('--deform-min-step', dest='deform_min_step',
                             type=float, default=0., help='Minimum step')
    deform_args.add_argument('--deform-iterations', dest='deform_iterations',
                             type=int, default=25,
                             help='Number of iterations')

    args_parser.add_argument('--output-dir', dest='output_dir',
                             required=True,
                             help='Output directory')
    args_parser.add_argument('--lowres-registration-steps', dest='lowres_registration_steps',
                             default='ransac,affine',
                             type=_stringlist,
                             help='Low res registration steps: (ransac,affine)')
    args_parser.add_argument('--highres-registration-steps', dest='highres_registration_steps',
                             default='ransac,deform',
                             type=_stringlist,
                             help='High res registration steps: (ransac,deform)')
    args_parser.add_argument('--highres-blocksize', dest='highres_blocksize',
                             default=[128,]*3,
                             type=_intlist,
                             help='High blocksize for partitioning the work')

    args_parser.add_argument('--dask-scheduler', dest='dask_scheduler',
                             type=str, default=None,
                             help='Run with distributed scheduler')

    args_parser.add_argument('--debug', dest='debug',
                             action='store_true',
                             help='Save some intermediate images for debugging')

    return args_parser


def _extract_ransac_args(args):
    ransac_args = {}
    if args.ransac_blob_sizes is not None:
        ransac_args['blob_sizes'] = args.ransac_blob_sizes
    if args.ransac_threshold is not None:
        ransac_args['threshold'] = args.ransac_threshold
    if args.ransac_confidence is not None:
        ransac_args['confidence'] = args.ransac_confidence
    return ransac_args


def _extract_affine_args(args):
    affine_args = {'optimizer_args': {}}
    if args.affine_shrink_factors is not None:
        affine_args['shrink_factors'] = args.affine_shrink_factors
    if args.affine_smooth_sigmas is not None:
        affine_args['smooth_sigmas'] = args.affine_smooth_sigmas
    if args.affine_learning_rate is not None:
        affine_args['optimizer_args']['learningRate'] = args.affine_learning_rate
    if args.affine_min_step is not None:
        affine_args['optimizer_args']['minStep'] = args.affine_min_step
    if args.affine_iterations is not None:
        affine_args['optimizer_args']['numberOfIterations'] = args.affine_iterations
    return affine_args


def _extract_deform_args(args):
    deform_args = {'optimizer_args': {}}
    if args.deform_control_point_spacing is not None:
        deform_args['control_point_spacing'] = args.deform_control_point_spacing
    if args.deform_control_point_levels is not None:
        deform_args['control_point_levels'] = args.deform_control_point_levels
    if args.deform_smooth_sigmas is not None:
        deform_args['smooth_sigmas'] = args.deform_smooth_sigmas
    if args.deform_learning_rate is not None:
        deform_args['optimizer_args']['learningRate'] = args.deform_learning_rate
    if args.deform_min_step is not None:
        deform_args['optimizer_args']['minStep'] = args.deform_min_step
    if args.deform_iterations is not None:
        deform_args['optimizer_args']['numberOfIterations'] = args.deform_iterations
    return deform_args


def _run_lowres_alignment(fix_data,
                          mov_data,
                          fix_spacing,
                          mov_spacing,
                          steps):
    print('Run low res alignment:', steps)

    affine = alignment_pipeline(fix_data,
                                mov_data,
                                fix_spacing,
                                mov_spacing,
                                steps)

    print('Apply affine transform')
    # apply transform
    aligned = apply_transform(fix_data,
                              mov_data,
                              fix_spacing,
                              mov_spacing,
                              transform_list=[affine,])

    return affine, aligned


def _run_highres_alignment(fix_data,
                           mov_data,
                           fix_spacing,
                           mov_spacing,
                           steps,
                           blocksize,
                           transforms_list,
                           output_dir,
                           cluster):
    print('Run high res alignment:', steps, blocksize)
    deform_output = output_dir + '/deformed.zarr'
    deform = distributed_piecewise_alignment_pipeline(
        fix_data, mov_data,
        fix_spacing, mov_spacing,
        steps,
        blocksize=blocksize,
        static_transform_list=transforms_list,
        write_path=deform_output,
        cluster=cluster,
    )

    aligned = distributed_apply_transform(
        fix_data, mov_data,
        fix_spacing, mov_spacing,
        transform_list=transforms_list + [deform],
        blocksize=blocksize,
        write_path=deform_output,
        cluster=cluster,
    )

    return deform, aligned


if __name__ == '__main__':
    args_parser = _define_args()
    args = args_parser.parse_args()

    args_for_steps = {
        'ransac': _extract_ransac_args(args),
        'affine': _extract_affine_args(args),
        'deform': _extract_deform_args(args),
    }

    print('Run registration with:', args, args_for_steps)

    # Read the the lowres inputs
    fix_lowres_ldata, fix_lowres_attrs = n5_utils.open(
        args.fixed_lowres, args.fixed_lowres_subpath)
    mov_lowres_ldata, mov_lowres_attrs = n5_utils.open(
        args.moving_lowres, args.moving_lowres_subpath)
    fix_lowres_voxel_spacing = n5_utils.get_voxel_spacing(fix_lowres_attrs)
    mov_lowres_voxel_spacing = n5_utils.get_voxel_spacing(mov_lowres_attrs)

    # Read the highres inputs - if highres is not defined default it to lowres
    fix_highres_path = args.fixed_highres if args.fixed_highres else args.fixed_lowres
    mov_highres_path = args.fixed_highres if args.fixed_highres else args.fixed_lowres

    fix_highres_ldata, fix_highres_attrs = n5_utils.open(
        fix_highres_path, args.fixed_highres_subpath)
    mov_highres_ldata, mov_highres_attrs = n5_utils.open(
        mov_highres_path, args.moving_highres_subpath)
    fix_highres_voxel_spacing = n5_utils.get_voxel_spacing(fix_highres_attrs)
    mov_highres_voxel_spacing = n5_utils.get_voxel_spacing(mov_highres_attrs)

    print('Fixed lowres volume attributes:',
          fix_lowres_ldata.shape, fix_lowres_voxel_spacing)
    print('Moving lowres volume attributes:',
          mov_lowres_ldata.shape, mov_lowres_voxel_spacing)

    print('Fixed highres volume attributes:',
          fix_highres_ldata.shape, fix_highres_voxel_spacing)
    print('Moving highres volume attributes:',
          mov_highres_ldata.shape, mov_highres_voxel_spacing)

    lowres_steps = [(s, args_for_steps.get(s, {}))
                    for s in args.lowres_registration_steps]

    lowres_transform, lowres_alignment = _run_lowres_alignment(
        fix_lowres_ldata[...],  # read image in memory
        mov_lowres_ldata[...],
        fix_lowres_voxel_spacing,
        mov_lowres_voxel_spacing,
        lowres_steps)

    # save the lowres transformation
    np.savetxt(args.output_dir + '/affine.mat', lowres_transform)

    if (args.debug):
        nrrd.write(args.output_dir + '/affine.nrrd',
                   lowres_alignment.transpose(2, 1, 0), compression_level=2)

    highres_steps = [(s, args_for_steps.get(s, {}))
                     for s in args.highres_registration_steps]

    if args.dask_scheduler:
        cluster = remote_cluster(args.dask_scheduler)
    else:
        cluster = local_cluster()

    _run_highres_alignment(
        fix_highres_ldata,
        mov_highres_ldata,
        fix_highres_voxel_spacing,
        mov_highres_voxel_spacing,
        highres_steps,
        args.highres_blocksize,
        [lowres_transform,],
        args.output_dir,
        cluster
    )
