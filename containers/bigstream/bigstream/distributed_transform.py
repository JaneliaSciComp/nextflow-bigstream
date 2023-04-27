import dask.array as da
import numpy as np
import os
import tempfile

import bigstream.transform as cs_transform
import bigstream.utility as ut

from itertools import product
from ClusterWrap.decorator import cluster
from dask.utils import SerializableLock


@cluster
def distributed_apply_transform(
    fix, mov,
    fix_spacing, mov_spacing,
    partition_size,
    output_chunk_size,
    transform_list,
    overlap_factor=0.5,
    aligned_dataset=None,
    transform_spacing=None,
    cluster=None,
    cluster_kwargs={},
    **kwargs,
):
    """
    Resample a larger-than-memory moving image onto a fixed image through a
    list of transforms

    Parameters
    ----------
    fix_zarr : zarr array
        The fixed image data

    mov_zarr : zarr array
        The moving image data

    fix_spacing : 1d array
        The spacing in physical units (e.g. mm or um) between voxels
        of the fixed image. Length must equal `fix.ndim`

    mov_spacing : 1d array
        The spacing in physical units (e.g. mm or um) between voxels
        of the moving image. Length must equal `mov.ndim`

    partition_size : int
        The block size used for distributing the work

    output_chunk_size :
        Output chunk size

    transform_list : list
        The list of transforms to apply. These may be 2d arrays of shape 4x4
        (affine transforms), or ndarrays of `fix.ndim` + 1 dimensions (deformations).
        Zarr arrays work just fine.
        If they are ndarrays there's no need to save these as temporary zarr since they
        already come either from a zarr or N5 container

    overlap_factor : float in range [0, 1] (default: 0.5)
        Block overlap size as a percentage of block size

    aligned_dataset : ndarray (default: None)
        A subpath in the zarr array to write the resampled data to

    transform_spacing : tuple
        Spacing to be applied for each transform. If not set
        it uses the fixed spacing

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
        The resampled moving data with transform_list applied. 
        If aligned_dataset is not None this will be the output
        Otherwise it returns a numpy array.
    """

    # get overlap and number of blocks
    partition_dims = np.array((partition_size,)*fix.ndim)
    nblocks = np.ceil(np.array(fix.shape) / partition_dims).astype(int)
    overlaps = np.round(partition_dims * overlap_factor).astype(int)

    # ensure there's a 1:1 correspondence between transform spacing 
    # and transform list
    if transform_spacing is None:
        # create transform spacing using same value as fixed image
        transform_spacing_list = ((np.array(fix_spacing),) * 
            len(transform_list))
    elif not isinstance(transform_spacing, tuple):
        # create a corresponding transform spacing for each transform
        transform_spacing_list = ((transform_spacing,) *
            len(transform_list))
    else:
        # transform spacing is a tuple
        # assume it's length matches transform list length
        transform_spacing_list = transform_spacing

    # store block coordinates in a dask array
    blocks_coords = np.empty(nblocks, dtype=list)
    for (i, j, k) in np.ndindex(*nblocks):
        start = partition_dims * (i, j, k) - overlaps
        stop = start + partition_dims + 2 * overlaps
        start = np.maximum(0, start)
        stop = np.minimum(fix.shape, stop)
        block_coords = tuple(slice(x, y) for x, y in zip(start, stop))
        blocks_coords[i, j, k] = block_coords
    blocks_coords_arr = da.from_array(blocks_coords, 
                                      chunks=(1,)*blocks_coords.ndim)
    # align all blocks
    aligned = da.map_blocks(
        _transform_single_block,
        blocks_coords_arr,
        fix=fix,
        mov=mov,
        fix_spacing=fix_spacing,
        mov_spacing=mov_spacing,
        blocksize=partition_dims,
        blockoverlaps=overlaps,
        transform_list=transform_list,
        transform_spacing_list=transform_spacing_list,
        dtype=fix.dtype,
        chunks=output_chunk_size,
        *kwargs
    )

    # crop to original size
    aligned = aligned[tuple(slice(s) for s in fix.shape)]

    # return
    if aligned_dataset is not None:
        lock = SerializableLock()
        da.store(aligned, aligned_dataset, lock=lock)
        return aligned_dataset
    else:
        return aligned.compute()


def _transform_single_block(block_coords,
                            fix=None,
                            mov=None,
                            fix_spacing=None,
                            mov_spacing=None,
                            blocksize=None,
                            blockoverlaps=None,
                            transform_list=[],
                            transform_spacing_list=[],
                            **additional_transform_args):
    """
    Block transform function
    """
    print('Transform block: ', block_coords)

    # fetch fixed image slices and read fix
    fix_slices = block_coords.item()
    fix_block = fix[fix_slices]
    fix_origin = fix_spacing * [s.start for s in fix_slices]

    # read relevant region of transforms
    applied_transform_list = []
    transform_origin = [fix_origin,] * len(transform_list)
    for iii, transform in enumerate(transform_list):
        if transform.shape != (4, 4):
            start = np.floor(fix_origin / transform_spacing_list[iii]).astype(int)
            stop = [s.stop for s in fix_slices] * fix_spacing / transform_spacing_list[iii]
            stop = np.ceil(stop).astype(int)
            transform = transform[tuple(slice(a, b) for a, b in zip(start, stop))]
            transform_origin[iii] = start * transform_spacing_list[iii]
        applied_transform_list.append(transform)
    transform_origin = tuple(transform_origin)

    # transform fixed block corners, read moving data
    fix_block_coords = []
    for corner in list(product([0, 1], repeat=3)):
        a = [x.stop-1 if y else x.start for x, y in zip(fix_slices, corner)]
        fix_block_coords.append(a)
    fix_block_coords = np.array(fix_block_coords) * fix_spacing
    mov_block_coords = cs_transform.apply_transform_to_coordinates(
        fix_block_coords, applied_transform_list, transform_spacing_list, transform_origin,
    )
    mov_block_coords = np.round(mov_block_coords / mov_spacing).astype(int)
    mov_block_coords = np.maximum(0, mov_block_coords)
    mov_block_coords = np.minimum(mov.shape, mov_block_coords)
    mov_start = np.min(mov_block_coords, axis=0)
    mov_stop = np.max(mov_block_coords, axis=0)
    mov_slices = tuple(slice(a, b) for a, b in zip(mov_start, mov_stop))
    mov_block = mov[mov_slices]
    mov_origin = mov_spacing * [s.start for s in mov_slices]

    # resample
    aligned = cs_transform.apply_transform(
        fix_block, mov_block, 
        fix_spacing, mov_spacing,
        transform_list=applied_transform_list,
        transform_origin=transform_origin,
        fix_origin=fix_origin,
        mov_origin=mov_origin,
        **additional_transform_args,
    )

    # crop out overlap
    for axis in range(aligned.ndim):
        # left side
        slc = [slice(None),]*aligned.ndim
        if fix_slices[axis].start != 0:
            slc[axis] = slice(blockoverlaps[axis], None)
            aligned = aligned[tuple(slc)]

        # right side
        slc = [slice(None),]*aligned.ndim
        if aligned.shape[axis] > blocksize[axis]:
            slc[axis] = slice(None, blocksize[axis])
            aligned = aligned[tuple(slc)]

    # return result
    return aligned


@cluster
def distributed_apply_transform_to_coordinates(
    coordinates,
    transform_list,
    partition_size=30.,
    coords_spacing=None,
    coords_origin=None,
    temporary_directory=None,
    cluster=None,
    cluster_kwargs={},
):
    """
    Move a set of coordinates through a list of transforms
    Transforms can be larger-than-memory

    Parameters
    ----------
    coordinates : Nxd array
        The coordinates to move. N such coordinates in d dimensions.

    transform_list : list
        The transforms to apply, in stack order. Elements must be 2d 4x4 arrays
        (affine transforms) or d + 1 dimension arrays (deformations).
        Zarr arrays work just fine.

    partition_size : scalar float (default: 30.)
        Size of blocks that domain is carved into for distributed computation
        in same units as coordinates

    transform_spacing : 1d array or tuple of 1d arrays (default: None)
        The spacing in physical units (e.g. mm or um) between voxels
        of any deformations in the transform_list. If any transform_list
        contains any deformations then transform_spacing cannot be None.
        If a single 1d array then all deforms have that spacing.
        If a tuple, then its length must be the same as transform_list,
        thus each deformation can be given its own spacing. Spacings given
        for affine transforms are ignored.

    transform_origin : 1d array or tuple of 1d arrays (default: None)
        The origin in physical units (e.g. mm or um) of the given transforms.
        If None, all origins are assumed to be (0, 0, 0, ...); otherwise, follows
        the same logic as transform_spacing. Origins given for affine transforms
        are ignored.

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

    Returns
    -------
    transformed_coordinates : Nxd array
        The given coordinates transformed by the given transform_list
    """

    # temporary file paths and ensure inputs are zarr
    temporary_directory = tempfile.TemporaryDirectory(
        prefix='.', dir=temporary_directory or os.getcwd(),
    )

    # determine partitions of coordinates
    origin = np.min(coordinates[:, 0:3], axis=0)
    maxblock = np.max(coordinates[:, 0:3], axis=0)
    blocksdims = maxblock - origin
    nblocks = np.ceil(blocksdims / partition_size).astype(int)
    print('Min:', origin, 'Max:', maxblock, 
          'Dims:', blocksdims, 'NBlocks:', nblocks)
    partitions = []
    for (i, j, k) in np.ndindex(*nblocks):
        lower_bound = origin + partition_size * np.array((i, j, k))
        upper_bound = lower_bound + partition_size
        not_too_low = np.all(coordinates[:, 0:3] >= lower_bound, axis=1)
        not_too_high = np.all(coordinates[:, 0:3] < upper_bound, axis=1)
        pcoords = coordinates[ not_too_low * not_too_high ]
        if pcoords.size != 0: partitions.append(pcoords)

    # transform all partitions and return
    futures = cluster.client.map(
        _transform_coords,
        partitions,
        coords_spacing=coords_spacing,
        transform_list=transform_list,
    )

    future_keys = [f.key for f in futures]

    results = cluster.client.gather(futures)

    return np.concatenate(results, axis=0)


def _transform_coords(coord_indexed_values, coords_spacing, transform_list):
    # read relevant region of transform
    point_coords = coord_indexed_values[:, 0:3]
    a = np.min(point_coords, axis=0)
    b = np.max(point_coords, axis=0)
    cropped_transforms = []
    for iii, transform in enumerate(transform_list):
        if transform.shape != (4, 4):
            # for vector displacement fields crop the transformation
            spacing = coords_spacing
            if isinstance(spacing, tuple): spacing = spacing[iii]
            start = np.floor(a / spacing).astype(int)
            stop = np.ceil(b / spacing).astype(int) + 1
            crop = tuple(slice(x, y) for x, y in zip(start, stop))
            cropped_transform = transform[crop]
            cropped_transforms.append(cropped_transform)
        else:
            # no need to do any cropping if it's an affine matrix
            cropped_transforms.append(transform)

    # apply transforms
    warped_point_coords = cs_transform.apply_transform_to_coordinates(
        point_coords,
        cropped_transforms,
        coords_spacing,
        transform_origin=a,
    )
    points_values = coord_indexed_values[:, 3:]

    if points_values.shape[1] == 0:
        return warped_coords_indexed_values
    else:
        warped_coords_indexed_values = np.empty_like(coord_indexed_values)
        warped_coords_indexed_values[:, 0:3] = warped_point_coords
        warped_coords_indexed_values[:, 3:] = points_values
        return warped_coords_indexed_values
