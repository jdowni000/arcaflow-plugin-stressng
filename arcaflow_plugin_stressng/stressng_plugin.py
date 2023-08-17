#!/usr/bin/env python3

import sys
import typing
import tempfile
import yaml
import subprocess
import os

from arcaflow_plugin_sdk import plugin
from stressng_schema import (
    WorkloadParams,
    WorkloadResults,
    WorkloadError,
    system_info_output_schema,
    cpu_output_schema,
    vm_output_schema,
    matrix_output_schema,
    mq_output_schema,
    hdd_output_schema,
)


@plugin.step(
    id="workload",
    name="stress-ng workload",
    description="Run the stress-ng workload with the given parameters",
    outputs={"success": WorkloadResults, "error": WorkloadError},
)
def stressng_run(
    params: WorkloadParams,
) -> typing.Tuple[str, typing.Union[WorkloadResults, WorkloadError]]:

    print("==>> Generating temporary jobfile...")
    # generic parameters are in the StressNGParams class (e.g. the timeout)
    result = params.StressNGParams.to_jobfile()
    # now we need to iterate of the list of stressors
    for item in params.StressNGParams.stressors:
        result = result + item.to_jobfile()

    stressng_jobfile = tempfile.mkstemp()
    stressng_outfile = tempfile.mkstemp()

    # write the temporary jobfile
    try:
        with open(stressng_jobfile[1], "w") as jobfile:
            try:
                jobfile.write(result)
            except IOError as error:
                return "error", WorkloadError(
                    f"{error} while trying to write {stressng_jobfile[1]}"
                )
    except EnvironmentError as error:
        return "error", WorkloadError(
            f"{error} while trying to open {stressng_jobfile[1]}"
        )

    stressng_command = [
        "/usr/bin/stress-ng",
        "-j",
        stressng_jobfile[1],
        "--metrics",
        "-Y",
        stressng_outfile[1],
    ]

    print("==>> Running stress-ng with the temporary jobfile...")
    workdir = "/tmp"
    if params.StressNGParams.workdir is not None:
        workdir = params.StressNGParams.workdir
    try:
        print(
            subprocess.check_output(
                stressng_command,
                cwd=workdir,
                text=True,
                stderr=subprocess.STDOUT,
            )
        )
    except subprocess.CalledProcessError as error:
        return "error", WorkloadError(
            f"""{error.cmd[0]} failed with return code
                {error.returncode}:\n{error.output}"""
        )

    try:
        with open(stressng_outfile[1], "r") as output:
            try:
                stressng_yaml = yaml.safe_load(output)
            except yaml.YAMLError as error:
                print(error)
                return "error", WorkloadError(
                    f"""{error} in
                                                  {stressng_outfile[1]}"""
                )
    except EnvironmentError as error:
        return "error", WorkloadError(
            f"{error} while trying to open {stressng_outfile[1]}"
        )

    system_info = stressng_yaml["system-info"]
    metrics = stressng_yaml["metrics"]

    # allocate all stressor information with None in case they don't get called
    cpuinfo_un = None
    vminfo_un = None
    matrixinfo_un = None
    mqinfo_un = None
    hddinfo_un = None

    system_un = system_info_output_schema.unserialize(system_info)
    for metric in metrics:
        if metric["stressor"] == "cpu":
            cpuinfo_un = cpu_output_schema.unserialize(metric)
        if metric["stressor"] == "vm":
            vminfo_un = vm_output_schema.unserialize(metric)
        if metric["stressor"] == "matrix":
            matrixinfo_un = matrix_output_schema.unserialize(metric)
        if metric["stressor"] == "mq":
            mqinfo_un = mq_output_schema.unserialize(metric)
        if metric["stressor"] == "hdd":
            hddinfo_un = hdd_output_schema.unserialize(metric)

    print("==>> Workload run complete!")
    os.close(stressng_jobfile[0])
    os.close(stressng_outfile[0])

    if params.cleanup:
        print("==>> Cleaning up operation files...")
        os.remove(stressng_jobfile[1])

    return "success", WorkloadResults(
        system_un,
        vminfo_un,
        cpuinfo_un,
        matrixinfo_un,
        mqinfo_un,
        hddinfo_un,
    )


if __name__ == "__main__":
    sys.exit(
        plugin.run(
            plugin.build_schema(
                stressng_run,
            )
        )
    )
