import numpy as np
import zarr


def open(n5_path, n5_subpath):
    n5_container = zarr.open(store=zarr.N5Store(n5_path), mode='r')
    a = n5_container[n5_subpath] if n5_subpath else n5_container
    return a, a.attrs.asdict()


def get_voxel_spacing(attrs):
    if (attrs.get('downsamplingFactors')):
        voxel_spacing = np.array(attrs['pixelResolution']) * np.array(attrs['downsamplingFactors'])
    else:
        voxel_spacing = np.array(attrs['pixelResolution']['dimensions'])
    return voxel_spacing[::-1] # put in zyx order
