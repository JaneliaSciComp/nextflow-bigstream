process BIGSTREAM {
    container { params.container }
    containerOptions { get_runtime_opts([inputPath]) }

    memory { "${params.mem_gb} GB" }
    cpus { params.cpus }

    input:
    tuple val(fixed_lowres_path),
          val(fixed_lowres_subpath),
          val(moving_lowres_path),
          val(moving_lowres_subpath)
          val(dataset), val(downsamplingFactors), val(pixelRes), val(pixelResUnits), val(scheduler), val(scheduler_workdir)

    output:
    tuple val(inputPath), val(scheduler), val(scheduler_workdir)

    script:
    def scheduler_arg = scheduler
        ? "--dask-scheduler ${scheduler}"
        : ''
    """
    /entrypoint.sh n5_multiscale \
        -i $inputPath -d $dataset \
        -f $downsamplingFactors -p $pixelRes -u $pixelResUnits \
        ${scheduler_arg}
    """
}
