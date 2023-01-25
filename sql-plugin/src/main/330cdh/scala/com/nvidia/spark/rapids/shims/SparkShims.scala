/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import org.apache.spark.sql.execution.SparkPlan
//import org.apache.spark.sql.hive.execution.HiveTableScanExec

import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, RunnableCommand}

//object HiveOverrides {
//
//  def getHiveTableExecRule() = {
//    GpuOverrides.exec[HiveTableScanExec](
//      desc = "CDH-specific override to disable HiveTableScanExec, " +
//        "to intercept Hive delimited text table reads.",
//      ExecChecks(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
//        TypeSig.all),
//      (p, conf, parent, r) => new SparkPlanMeta[HiveTableScanExec](p, conf, parent, r) {
//        override def convertToGpu(): GpuExec = null // No substitutions.
//        override def tagPlanForGpu(): Unit = {
//          willNotWorkOnGpu("No soup for you! HiveTableScanExec disabled on CDH.")
//        }
//      }
//    )
//  }
//}

object SparkShimImpl extends Spark330PlusNonDBShims with AnsiCastRuleShims {
  override def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = {
    Seq(GpuOverrides.dataWriteCmd[CreateDataSourceTableAsSelectCommand](
    "Create table with select command",
    (a, conf, p, r) => new CreateDataSourceTableAsSelectCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap
  }

  override def getRunnableCmds: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Map.empty
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {

    super.getExecs ++ Seq(org.apache.spark.sql.hive.cdh.HiveOverrides.getHiveTableExecRule())
      .map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

}
