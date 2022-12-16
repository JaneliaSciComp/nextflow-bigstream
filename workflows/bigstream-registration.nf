
include {
    dask_params;
} from '../params/dask_params';

dask_cluster_params = dask_params() + params

include {
    CREATE_DASK_CLUSTER;
} from '../external-modules/dask/workflows/create_dask_cluster' addParams(dask_cluster_params)

include {
    DASK_CLUSTER_TERMINATE;
} from '../external-modules/dask/modules/dask/cluster_terminate/main' addParams(dask_cluster_params)

include { get_runtime_opts } from '../utils' 

workflow BIGSTREAM_REGISTRATION {
    start_cluster
    | map {
        def (cluster_id, scheduler_ip, cluster_work_dir, connected_workers) = it
        [
            file(params.inputPath),
            params.dataset,
            params.downsamplingFactors,
            params.pixelRes,
            params.pixelResUnits,
            scheduler_ip,
            cluster_work_dir,
        ]
    }
    | BIGSTREAM
    | groupTuple(by: [1,2]) // group all processes that run on the same cluster
    | map { 
        def (input_paths, scheduler_ip, cluster_work_dir) = it
        return cluster_work_dir
    }
    | stop_cluster
}
