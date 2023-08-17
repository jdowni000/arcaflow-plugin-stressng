#!/usr/bin/env python3

import unittest
import yaml
import stressng_schema
import stressng_plugin
from arcaflow_plugin_sdk import plugin


class StressNGTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            stressng_schema.CpuStressorParams(
                stressor=stressng_schema.Stressors.CPU, cpu_count=2
            )
        )

        plugin.test_object_serialization(
            stressng_schema.VmStressorParams(
                stressor=stressng_schema.Stressors.VM, vm=2, vm_bytes="2G"
            )
        )

        plugin.test_object_serialization(
            stressng_schema.MatrixStressorParams(
                stressor=stressng_schema.Stressors.MATRIX, matrix=2
            )
        )
        plugin.test_object_serialization(
            stressng_schema.MqStressorParams(
                stressor=stressng_schema.Stressors.MQ, mq=2
            )
        )

    def test_functional_cpu(self):
        # idea is to run a small cpu bound benchmark and
        # compare its output with a known-good output
        # this is clearly not perfect, as we're limited to the
        # field names and can't do a direct
        # comparison of the returned values

        cpu = stressng_schema.CpuStressorParams(
            stressor="cpu", cpu_count=2, cpu_method="all"
        )

        stress = stressng_schema.StressNGParams(timeout="10s", stressors=[cpu])

        reference_jobfile = "tests/reference_jobfile_cpu"

        result = stress.to_jobfile()

        for item in stress.stressors:
            result = result + item.to_jobfile()

        with open(reference_jobfile, "r") as file:
            try:
                reference = yaml.safe_load(file)
            except yaml.YAMLError as e:
                print(e)

        self.assertEqual(yaml.safe_load(result), reference)
        workload_params = stressng_schema.WorkloadParams(stress, True)
        res = stressng_plugin.stressng_run(workload_params)
        self.assertIn("success", res)
        self.assertEqual(res[1].cpuinfo.stressor, "cpu")
        self.assertGreaterEqual(res[1].cpuinfo.wall_clock_time, 10)

    def test_functional_vm(self):
        vm = stressng_schema.VmStressorParams(
            stressor="vm", vm=1, vm_bytes="100m", mmap="1", mmap_bytes="10m"
        )

        stress = stressng_schema.StressNGParams(timeout="10s", stressors=[vm])

        reference_jobfile = "tests/reference_jobfile_vm"

        result = stress.to_jobfile()

        for item in stress.stressors:
            result = result + item.to_jobfile()

        with open(reference_jobfile, "r") as file:
            try:
                reference = yaml.safe_load(file)
            except yaml.YAMLError as e:
                print(e)

        self.assertEqual(yaml.safe_load(result), reference)
        workload_params = stressng_schema.WorkloadParams(stress, True)
        res = stressng_plugin.stressng_run(workload_params)
        self.assertIn("success", res)
        self.assertEqual(res[1].vminfo.stressor, "vm")
        self.assertGreaterEqual(res[1].vminfo.wall_clock_time, 10)

    def test_functional_matrix(self):
        matrix = stressng_schema.MatrixStressorParams(
            stressor="matrix",
            matrix=1,
        )

        stress = stressng_schema.StressNGParams(
            timeout="10s", stressors=[matrix]
        )

        reference_jobfile = "tests/reference_jobfile_matrix"

        result = stress.to_jobfile()

        for item in stress.stressors:
            result = result + item.to_jobfile()

        with open(reference_jobfile, "r") as file:
            try:
                reference = yaml.safe_load(file)
            except yaml.YAMLError as e:
                print(e)

        self.assertEqual(yaml.safe_load(result), reference)
        workload_params = stressng_schema.WorkloadParams(stress, True)
        res = stressng_plugin.stressng_run(workload_params)
        self.assertIn("success", res)
        self.assertEqual(res[1].matrixinfo.stressor, "matrix")
        self.assertGreaterEqual(res[1].matrixinfo.wall_clock_time, 9)

    def test_functional_mq(self):
        mq = stressng_schema.MqStressorParams(stressor="mq", mq=1)

        stress = stressng_schema.StressNGParams(timeout="10s", stressors=[mq])

        reference_jobfile = "tests/reference_jobfile_mq"

        result = stress.to_jobfile()

        for item in stress.stressors:
            result = result + item.to_jobfile()

        with open(reference_jobfile, "r") as file:
            try:
                reference = yaml.safe_load(file)
            except yaml.YAMLError as e:
                print(e)

        self.assertEqual(yaml.safe_load(result), reference)
        workload_params = stressng_schema.WorkloadParams(stress, True)
        res = stressng_plugin.stressng_run(workload_params)
        self.assertIn("success", res)
        self.assertEqual(res[1].mqinfo.stressor, "mq")
        self.assertGreaterEqual(res[1].mqinfo.wall_clock_time, 10)

    def test_functional_hdd(self):
        hdd = stressng_schema.HDDStressorParams(
            stressor="hdd", hdd=1, hdd_bytes="100m", hdd_write_size="4m"
        )

        stress = stressng_schema.StressNGParams(timeout="10s", stressors=[hdd])

        reference_jobfile = "tests/reference_jobfile_hdd"

        result = stress.to_jobfile()

        for item in stress.stressors:
            result = result + item.to_jobfile()

        with open(reference_jobfile, "r") as file:
            try:
                reference = yaml.safe_load(file)
            except yaml.YAMLError as e:
                print(e)

        self.assertEqual(yaml.safe_load(result), reference)
        workload_params = stressng_schema.WorkloadParams(stress, True)
        res = stressng_plugin.stressng_run(workload_params)
        self.assertIn("success", res)
        self.assertEqual(res[1].hddinfo.stressor, "hdd")
        self.assertGreaterEqual(res[1].hddinfo.wall_clock_time, 10)


if __name__ == "__main__":
    unittest.main()
