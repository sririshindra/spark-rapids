/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.rapids.spark

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.resource.{ResourceInformation, ResourceRequest}

/**
 *  A Spark Resource Discovery Plugin that relies on the Nvidia GPUs being in PROCESS_EXCLUSIVE
 *  mode so that it can discover free GPUs.
 *  This plugin iterates through all the GPUs on the node and tries to acquire them
 *  by doing a cudaFree(0) on each one. When the GPUs are in process exclusive mode this
 *  will result in that GPU being assigned to the specific process running this plugin and
 *  other executors will not be able to use it.
 *
 *  This plugin can be activated in spark with the configuration:
 *  --conf spark.resourceDiscovery.plugin=ai.rapids.spark.ExclusiveModeGpuDiscoveryPlugin
 */
class ExclusiveModeGpuDiscoveryPlugin extends ResourceDiscoveryPlugin with Logging {
  override def discoverResource(
      request: ResourceRequest,
      sparkconf: SparkConf): Optional[ResourceInformation] = {

    if (!request.id.resourceName.equals("gpu")) {
      logWarning("ExclusiveModeGpuDiscoveryPlugin only handles gpu allocations")
      return Optional.empty()
    }
    val ngpusRequested = request.amount
    val deviceCount: Int = Cuda.getDeviceCount()
    logInfo(s"Running ExclusiveModeGpuDiscoveryPlugin to acquire $ngpusRequested GPU(s), " +
      s" host has $deviceCount GPU(s)")
    // loop multiple times in case we have a race condition with another executor
    var numIters = ngpusRequested * 2
    val allocatedAddrs = ArrayBuffer[String]()
    val addrsToTry = ArrayBuffer.empty ++= (0 to (deviceCount - 1))
    while (numIters > 0 && allocatedAddrs.size < ngpusRequested && addrsToTry.size > 0) {
      logWarning(s"looping iteration $numIters")
      val gpuAllocated = addrsToTry.find(addr => GpuDeviceManager.tryToSetGpuDeviceAndAcquire(addr))
      gpuAllocated match {
        case Some(addr) =>
          addrsToTry -= addr
          allocatedAddrs += addr.toString
        case None =>
      }
      numIters -= 1
    }
    if (allocatedAddrs.size < ngpusRequested) {
      logWarning(s"ExclusiveModeGpuDiscoveryPlugin did not find enough gpus, " +
        s"requested: $ngpusRequested found: ${allocatedAddrs.size}")
    }
    Optional.of(new ResourceInformation("gpu", allocatedAddrs.toArray))
  }
}
