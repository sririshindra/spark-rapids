/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.sql.execution.FileSourceScanExec

object GpuReadOrcFileFormat {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val fsse = meta.wrapped
    if (fsse.relation.options.getOrElse("mergeSchema", "false").toBoolean) {
      meta.willNotWorkOnGpu("mergeSchema and schema evolution is not supported yet")
    }
    GpuOrcScanBase.tagSupport(
      fsse.sqlContext.sparkSession,
      fsse.requiredSchema,
      meta
    )
  }
}
