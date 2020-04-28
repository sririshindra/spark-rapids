# Copyright (c) 2020, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from conftest import is_incompat
import math
from pyspark.sql import Row
import pytest
from spark_session import with_cpu_session, with_gpu_session
import time
import types as pytypes

def _assert_equal(cpu, gpu, incompat, path):
    t = type(cpu)
    if (t is Row):
        assert len(cpu) == len(gpu), "CPU and GPU row have different lengths at {}".format(path)
        if hasattr(cpu, "__fields__") and hasattr(gpu, "__fields__"):
            for field in cpu.__fields__:
                _assert_equal(cpu[field], gpu[field], incompat, path + [field])
        else:
            for index in range(len(cpu)):
                _assert_equal(cpu[index], gpu[index], incompat, path + [index])
    elif (t is list):
        assert len(cpu) == len(gpu), "CPU and GPU list have different lengths at {}".format(path)
        for index in range(len(cpu)):
            _assert_equal(cpu[index], gpu[index], incompat, path + [index])
    elif (t is pytypes.GeneratorType):
        index = 0
        # generator has no zip :( so we have to do this the hard way
        done = False
        while not done:
            sub_cpu = None
            sub_gpu = None
            try:
                sub_cpu = next(cpu)
            except StopIteration:
                done = True

            try:
                sub_gpu = next(gpu)
            except StopIteration:
                done = True

            if done:
                assert sub_cpu == sub_gpu and sub_cpu == None, "CPU and GPU generators have different lengths at {}".format(path)
            else:
                _assert_equal(sub_cpu, sub_gpu, incompat, path + [index])

            index = index + 1
    elif (t is int):
        assert cpu == gpu, "GPU and CPU int values are different at {}".format(path)
    elif (t is float):
        if (math.isnan(cpu)):
            assert math.isnan(gpu), "GPU and CPU float values are different at {}".format(path)
        elif (incompat):
            assert cpu == pytest.approx(gpu), "GPU and CPU float values are different even with approximation {}".format(path)
        else:
            assert cpu == gpu, "GPU and CPU float values are different at {}".format(path)
    elif isinstance(cpu, str):
        assert cpu == gpu, "GPU and CPU string values are different at {}".format(path)
    elif (cpu == None):
        assert cpu == gpu, "GPU and CPU are not both null at {}".format(path)
    else:
        assert False, "Found unexpected type {} at {}".format(t, path)

def assert_equal(cpu, gpu):
    """Verify that the result from the CPU and the GPU are equal"""
    _assert_equal(cpu, gpu, incompat=is_incompat(), path=[])

def _has_incompat_conf(conf):
    return ('spark.rapids.sql.incompatibleOps.enabled' in conf and
            conf['spark.rapids.sql.incompatibleOps.enabled'].lower() == 'true')

def _assert_gpu_and_cpu_are_equal(func,
        should_collect,
        conf={},
        sort_result=False,
        non_gpu_allowed=None):
    assert not sort_result, "sorting results is not supported yet"
    #TODO sort first if needed
    if should_collect:
        bring_back = lambda spark: func(spark).collect()
        collect_type = 'COLLECT'
    else:
        bring_back = lambda spark: func(spark).toLocalIterator()
        collect_type = 'ITERATOR'

    if is_incompat():
        conf = dict(conf) # Make a copy before we change anything
        conf['spark.rapids.sql.incompatibleOps.enabled'] = 'true'
    elif _has_incompat_conf(conf):
        raise AssertionError("incompat must be enabled by the incompat fixture")

    print('### CPU RUN ###')
    cpu_start = time.time()
    from_cpu = with_cpu_session(bring_back, conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    from_gpu = with_gpu_session(bring_back,
            conf=conf,
            non_gpu_allowed=non_gpu_allowed)
    gpu_end = time.time()
    print('### {}: GPU TOOK {} CPU TOOK {} ###'.format(collect_type, 
        gpu_end - gpu_start, cpu_end - cpu_start))
    assert_equal(from_cpu, from_gpu)

def assert_gpu_and_cpu_are_equal_collect(func,
        conf={},
        sort_result=False,
        non_gpu_allowed=None):
    """
    Assert when running func on both the CPU and the GPU that the results are equal.
    In this case the data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    """
    _assert_gpu_and_cpu_are_equal(func, True,
            conf=conf,
            sort_result=sort_result,
            non_gpu_allowed=non_gpu_allowed)

def assert_gpu_and_cpu_are_equal_iterator(func,
        conf={},
        sort_result=False,
        non_gpu_allowed=None):
    """
    Assert when running func on both the CPU and the GPU that the results are equal.
    In this case the data is pulled back to the driver in chunks and compared here
    so any amount of data can work, just be careful about how long it might take.
    """
    _assert_gpu_and_cpu_are_equal(func, False,
            conf=conf,
            sort_result=sort_result,
            non_gpu_allowed=non_gpu_allowed)
