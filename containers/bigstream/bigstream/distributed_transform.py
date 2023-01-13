import dask.array as da
import numpy as np
import os, tempfile
import zarr

import bigstream.transform as cs_transform
import bigstream.utility as ut

from itertools import product
from ClusterWrap.decorator import cluster
from dask.utils import SerializableLock


@cluster
def distributed_apply_transform(
    fix_zarr, mov_zarr,
    fix_spacing, mov_spacing,
    partition_size,
    output_blocks,
    transform_list,
    overlap_factor=0.5,
    inverse_transforms=False,
    aligned_dataset=None,
    temporary_directory=None,
    cluster=None,
    cluster_kwargs={},
    **kwargs,
):
    """
    Resample a larger-than-memory moving image onto a fixed image through a
    list of transforms

    Parameters
    ----------
    fix : zarr array
        The fixed image data

    mov : zarr array
        The moving image data

    fix_spacing : 1d array
        The spacing in physical units (e.g. mm or um) between voxels
        of the fixed image. Length must equal `fix.ndim`

    mov_spacing : 1d array
        The spacing in physical units (e.g. mm or um) between voxels
        of the moving image. Length must equal `mov.ndim`

    transform_list : list
        The list of transforms to apply. These may be 2d arrays of shape 4x4
        (affine transforms), or ndarrays of `fix.ndim` + 1 dimensions (deformations).
        Zarr arrays work just fine.

    partition_size : int
        The block size used for distributing the work

    overlap_factor : float in range [0, 1] (default: 0.5)
        Block overlap size as a percentage of block size

    inverse_transforms : bool (default: False)
        Set to true if the list of transforms are all inverted

    aligned_dataset : ndarray (default: None)
        A subpath in the zarr array to write the resampled data to

    temporary_directory : string (default: None)
        A parent directory for temporary data written to disk during computation
        If None then the current directory is used

    cluster : ClusterWrap.cluster object (default: None)
        Only set if you have constructed your own static cluster. The default behavior
        is to construct a cluster for the duration of this function, then close it
        when the function is finished.

    cluster_kwargs : dict (default: {})
        Arguments passed to ClusterWrap.cluster
        If working with an LSF cluster, this will be
        ClusterWrap.janelia_lsf_cluster. If on a workstation
        this will be ClusterWrap.local_cluster.
        This is how distribution parameters are specified.

    **kwargs : Any additional keyword arguments
        Passed to bigstream.transform.apply_transform

    Returns
    -------
    resampled : array
        The resampled moving data with transform_list applied. If write_path is not None
        this will be a zarr array. Otherwise it is a numpy array.
    """

    # temporary file paths and ensure inputs are zarr
    temporary_directory = tempfile.TemporaryDirectory(
        prefix='.', dir=temporary_directory or os.getcwd(),
    )
    fix_zarr_path = temporary_directory.name + '/fix.zarr'
    mov_zarr_path = temporary_directory.name + '/mov.zarr'
    fix_zarr = ut.numpy_to_zarr(fix_zarr, output_blocks, fix_zarr_path)
    mov_zarr = ut.numpy_to_zarr(mov_zarr, output_blocks, mov_zarr_path)

    # ensure all deforms are zarr
    new_list = []
    zarr_transform_blocks = output_blocks + (fix_zarr.ndim,)
    for iii, transform in enumerate(transform_list):
        if transform.shape != (4, 4):
            zarr_path = temporary_directory.name + f'/deform{iii}.zarr'
            transform = ut.numpy_to_zarr(transform, zarr_transform_blocks, zarr_path)
        new_list.append(transform)
    transform_list = new_list

    # ensure transform spacing is set explicitly
    if 'transform_spacing' not in kwargs.keys():
        kwargs['transform_spacing'] = np.array(fix_spacing)
    if not isinstance(kwargs['transform_spacing'], tuple):
        kwargs['transform_spacing'] = (kwargs['transform_spacing'],) * len(transform_list)

    # get overlap and number of blocks
    partition_dims = np.array((partition_size,)*fix_zarr.ndim)
    nblocks = np.ceil(np.array(fix_zarr.shape) / partition_dims).astype(int)
    overlaps = np.round(partition_dims * overlap_factor).astype(int)

    # store block coordinates in a dask array
    block_coords = np.empty(nblocks, dtype=tuple)
    for (i, j, k) in np.ndindex(*nblocks):
        start = partition_dims * (i, j, k) - overlaps
        stop = start + partition_dims + 2 * overlaps
        start = np.maximum(0, start)
        stop = np.minimum(fix_zarr.shape, stop)
        block_coords[i, j, k] = tuple(slice(x, y) for x, y in zip(start, stop))
    block_coords = da.from_array(block_coords, chunks=(1,)*block_coords.ndim)

    # pipeline to run on each block
    def transform_single_block(coords, transform_list):

        # fetch fixed image slices and read fix
        fix_slices = coords.item()
        fix = fix_zarr[fix_slices]
        fix_origin = fix_spacing * [s.start for s in fix_slices]

        # read relevant region of transforms
        new_list = []
        transform_origin = [fix_origin,] * len(transform_list)
        for iii, transform in enumerate(transform_list):
            if transform.shape != (4, 4):
                start = np.floor(fix_origin / kwargs['transform_spacing'][iii]).astype(int)
                stop = [s.stop for s in fix_slices] * fix_spacing / kwargs['transform_spacing'][iii]
                stop = np.ceil(stop).astype(int)
                transform = transform[tuple(slice(a, b) for a, b in zip(start, stop))]
                transform_origin[iii] = start * kwargs['transform_spacing'][iii]
            new_list.append(transform)
        transform_list = new_list
        transform_origin = tuple(transform_origin)

        # transform fixed block corners, read moving data
        fix_block_coords = []
        for corner in list(product([0, 1], repeat=3)):
            a = [x.stop-1 if y else x.start for x, y in zip(fix_slices, corner)]
            fix_block_coords.append(a)
        fix_block_coords = np.array(fix_block_coords) * fix_spacing
        mov_block_coords = cs_transform.apply_transform_to_coordinates(
            fix_block_coords, transform_list, kwargs['transform_spacing'], transform_origin,
        )
        mov_block_coords = np.round(mov_block_coords / mov_spacing).astype(int)
        mov_block_coords = np.maximum(0, mov_block_coords)
        mov_block_coords = np.minimum(mov_zarr.shape, mov_block_coords)
        mov_start = np.min(mov_block_coords, axis=0)
        mov_stop = np.max(mov_block_coords, axis=0)
        mov_slices = tuple(slice(a, b) for a, b in zip(mov_start, mov_stop))
        mov = mov_zarr[mov_slices]
        mov_origin = mov_spacing * [s.start for s in mov_slices]

        # resample
        aligned = cs_transform.apply_transform(
            fix, mov, fix_spacing, mov_spacing,
            transform_list=transform_list,
            transform_origin=transform_origin,
            fix_origin=fix_origin,
            mov_origin=mov_origin,
            **kwargs,
        )

        # crop out overlap
        for axis in range(aligned.ndim):

            # left side
            slc = [slice(None),]*aligned.ndim
            if fix_slices[axis].start != 0:
                slc[axis] = slice(overlaps[axis], None)
                aligned = aligned[tuple(slc)]

            # right side
            slc = [slice(None),]*aligned.ndim
            if aligned.shape[axis] > output_blocks[axis]:
                slc[axis] = slice(None, output_blocks[axis])
                aligned = aligned[tuple(slc)]

        # return result
        return aligned
    # END: closure

    # align all blocks
    aligned = da.map_blocks(
        transform_single_block,
        block_coords,
        transform_list=transform_list,
        dtype=fix_zarr.dtype,
        chunks=output_blocks,
    )

    # crop to original size
    aligned = aligned[tuple(slice(s) for s in fix_zarr.shape)]

    # return
    if aligned_dataset is not None:
        lock = SerializableLock()
        da.store(aligned, aligned_dataset, lock=lock)
        return aligned_dataset
    else:
        return aligned.compute()
