/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark320

import java.net.URI
import java.nio.ByteBuffer

import scala.collection.mutable.ListBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.nvidia.spark.ParquetCachedBatchSerializer
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.v2._
import com.nvidia.spark.rapids.spark320.RapidsShuffleManager
import org.apache.arrow.memory.ReferenceManager
import org.apache.arrow.vector.ValueVector
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Abs, Alias, AnsiCast, Attribute, Cast, ElementAt, Expression, ExprId, GetArrayItem, GetMapValue, Lag, Lead, Literal, NamedExpression, NullOrdering, PlanExpression, PythonUDF, RegExpReplace, ScalaUDF, SortDirection, SortOrder, SpecifiedWindowFrame, TimeAdd, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.{CommandResultExec, FileSourceScanExec, PartitionedFileUtil, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.{FileIndex, FilePartition, FileScanRDD, HadoopFsRelation, InMemoryFileIndex, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.rapids.GpuPartitioningUtils
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.rapids.execution.python.shims.v2.GpuFlatMapGroupsInPandasExecMeta
import org.apache.spark.sql.rapids.shims.v2.{GpuInMemoryTableScanExec, GpuTimeAdd, _}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.unsafe.types.CalendarInterval

class Spark320Shims extends Spark32XShims {
  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION320

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }

  override def getScalaUDFAsExpression(
      function: AnyRef,
      dataType: DataType,
      children: Seq[Expression],
      inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
      outputEncoder: Option[ExpressionEncoder[_]] = None,
      udfName: Option[String] = None,
      nullable: Boolean = true,
      udfDeterministic: Boolean = true): Expression = {
    ScalaUDF(function, dataType, children, inputEncoders, outputEncoder, udfName, nullable,
      udfDeterministic)
  }

  override def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId,
      startMapIndex, endMapIndex, startPartition, endPartition)
  }

  override def getGpuBroadcastNestedLoopJoinShim(
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression],
      targetSizeBytes: Long): GpuBroadcastNestedLoopJoinExecBase = {
    GpuBroadcastNestedLoopJoinExec(left, right, join, joinType, condition, targetSizeBytes)
  }

  override def isGpuBroadcastHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuBroadcastHashJoinExec => true
      case _ => false
    }
  }

  override def isWindowFunctionExec(plan: SparkPlan): Boolean = plan.isInstanceOf[WindowExecBase]

  override def isGpuShuffledHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuShuffledHashJoinExec => true
      case _ => false
    }
  }

  override def getFileSourceMaxMetadataValueLength(sqlConf: SQLConf): Int =
    sqlConf.maxMetadataStringLength

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[Cast](
      "Convert a column of one type of data into another type",
      new CastChecks(),
      (cast, conf, p, r) => new CastExprMeta[Cast](cast, SparkSession.active.sessionState.conf
          .ansiEnabled, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!conf.isCastFloatToIntegralTypesEnabled &&
              (fromType == DataTypes.FloatType || fromType == DataTypes.DoubleType) &&
              (toType == DataTypes.ByteType || toType == DataTypes.ShortType ||
                  toType == DataTypes.IntegerType || toType == DataTypes.LongType)) {
            willNotWorkOnGpu(buildTagMessage(RapidsConf.ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES))
          }
          super.tagExprForGpu()
        }

        override def stringToDateAnsiModeEnabled: Boolean = true
      }),
    GpuOverrides.expr[AnsiCast](
      "Convert a column of one type of data into another type",
      new CastChecks {
        import TypeSig._
        // nullChecks are the same

        override val booleanChecks: TypeSig = integral + fp + BOOLEAN + STRING
        override val sparkBooleanSig: TypeSig = numeric + BOOLEAN + STRING

        override val integralChecks: TypeSig = gpuNumeric + BOOLEAN + STRING
        override val sparkIntegralSig: TypeSig = numeric + BOOLEAN + STRING

        override val fpChecks: TypeSig = (gpuNumeric + BOOLEAN + STRING)
            .withPsNote(TypeEnum.STRING, fpToStringPsNote)
        override val sparkFpSig: TypeSig = numeric + BOOLEAN + STRING

        override val dateChecks: TypeSig = TIMESTAMP + DATE + STRING
        override val sparkDateSig: TypeSig = TIMESTAMP + DATE + STRING

        override val timestampChecks: TypeSig = TIMESTAMP + DATE + STRING
        override val sparkTimestampSig: TypeSig = TIMESTAMP + DATE + STRING

        // stringChecks are the same, but adding in PS note
        private val fourDigitYearMsg: String = "Only 4 digit year parsing is available. To " +
            s"enable parsing anyways set ${RapidsConf.HAS_EXTENDED_YEAR_VALUES} to false."
        override val stringChecks: TypeSig = gpuNumeric + BOOLEAN + STRING + BINARY +
            TypeSig.psNote(TypeEnum.DATE, fourDigitYearMsg) +
            TypeSig.psNote(TypeEnum.TIMESTAMP, fourDigitYearMsg)

        // binaryChecks are the same
        override val decimalChecks: TypeSig = gpuNumeric + STRING
        override val sparkDecimalSig: TypeSig = numeric + BOOLEAN + STRING

        // calendarChecks are the same

        override val arrayChecks: TypeSig =
          ARRAY.nested(commonCudfTypes + DECIMAL_64 + NULL + ARRAY + BINARY + STRUCT) +
              psNote(TypeEnum.ARRAY, "The array's child type must also support being cast to " +
                  "the desired child type")
        override val sparkArraySig: TypeSig = ARRAY.nested(all)

        override val mapChecks: TypeSig =
          MAP.nested(commonCudfTypes + DECIMAL_64 + NULL + ARRAY + BINARY + STRUCT + MAP) +
              psNote(TypeEnum.MAP, "the map's key and value must also support being cast to the " +
                  "desired child types")
        override val sparkMapSig: TypeSig = MAP.nested(all)

        override val structChecks: TypeSig =
          STRUCT.nested(commonCudfTypes + DECIMAL_64 + NULL + ARRAY + BINARY + STRUCT) +
              psNote(TypeEnum.STRUCT, "the struct's children must also support being cast to the " +
                  "desired child type(s)")
        override val sparkStructSig: TypeSig = STRUCT.nested(all)

        override val udtChecks: TypeSig = none
        override val sparkUdtSig: TypeSig = UDT
      },
      (cast, conf, p, r) => new CastExprMeta[AnsiCast](cast, true, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!conf.isCastFloatToIntegralTypesEnabled &&
              (fromType == DataTypes.FloatType || fromType == DataTypes.DoubleType) &&
              (toType == DataTypes.ByteType || toType == DataTypes.ShortType ||
                  toType == DataTypes.IntegerType || toType == DataTypes.LongType)) {
            willNotWorkOnGpu(buildTagMessage(RapidsConf.ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES))
          }
          super.tagExprForGpu()
        }
      }),
    GpuOverrides.expr[Average](
      "Average aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.DOUBLE, TypeSig.DOUBLE + TypeSig.DECIMAL_128_FULL,
        // NullType is not technically allowed by Spark, but in practice in 3.2.0
        // it can show up
        Seq(ParamCheck("input", TypeSig.integral + TypeSig.fp + TypeSig.NULL,
          TypeSig.numericAndInterval + TypeSig.NULL))),
      (a, conf, p, r) => new AggExprMeta[Average](a, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          val dataType = a.child.dataType
          GpuOverrides.checkAndTagFloatAgg(dataType, conf, this)
        }

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuAverage(childExprs.head)

        // Average is not supported in ANSI mode right now, no matter the type
        override val ansiTypeToCheck: Option[DataType] = None
      }),
    GpuOverrides.expr[Abs](
      "Absolute value",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes, TypeSig.gpuNumeric, TypeSig.numeric),
      (a, conf, p, r) => new UnaryAstExprMeta[Abs](a, conf, p, r) {
        val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagSelfForAst(): Unit = {
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST unary minus does not support ANSI mode.")
          }
        }

        // ANSI support for ABS was added in 3.2.0 SPARK-33275
        override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child, ansiEnabled)
      }),
    GpuOverrides.expr[RegExpReplace](
      "RegExpReplace support for string literal input patterns",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regex", TypeSig.lit(TypeEnum.STRING)
              .withPsNote(TypeEnum.STRING, "very limited regex support"), TypeSig.STRING),
          ParamCheck("rep", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("pos", TypeSig.lit(TypeEnum.INT)
              .withPsNote(TypeEnum.INT, "only a value of 1 is supported"),
            TypeSig.lit(TypeEnum.INT)))),
      (a, conf, p, r) => new ExprMeta[RegExpReplace](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (GpuOverrides.isNullOrEmptyOrRegex(a.regexp)) {
            willNotWorkOnGpu(
              "Only non-null, non-empty String literals that are not regex patterns " +
                  "are supported by RegExpReplace on the GPU")
          }
          GpuOverrides.extractLit(a.pos).foreach { lit =>
            if (lit.value.asInstanceOf[Int] != 1) {
              willNotWorkOnGpu("Only a search starting position of 1 is supported")
            }
          }
        }
        override def convertToGpu(): GpuExpression = {
          // ignore the pos expression which must be a literal 1 after tagging check
          require(childExprs.length == 4,
            s"Unexpected child count for RegExpReplace: ${childExprs.length}")
          val Seq(subject, regexp, rep) = childExprs.take(3).map(_.convertToGpu())
          GpuStringReplace(subject, regexp, rep)
        }
      }),
    // Spark 3.2.0-specific LEAD expression, using custom OffsetWindowFunctionMeta.
    GpuOverrides.expr[Lead](
      "Window function that returns N entries ahead of this one",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 +
                TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.NULL +
                TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all)
        )
      ),
      (lead, conf, p, r) => new OffsetWindowFunctionMeta[Lead](lead, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuLead(input.convertToGpu(), offset.convertToGpu(), default.convertToGpu())
      }),
    // Spark 3.2.0-specific LAG expression, using custom OffsetWindowFunctionMeta.
    GpuOverrides.expr[Lag](
      "Window function that returns N entries behind this one",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 +
                TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.NULL +
                TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all)
        )
      ),
      (lag, conf, p, r) => new OffsetWindowFunctionMeta[Lag](lag, conf, p, r) {
        override def convertToGpu(): GpuExpression = {
          GpuLag(input.convertToGpu(), offset.convertToGpu(), default.convertToGpu())
        }
      }),
    GpuOverrides.expr[GetArrayItem](
      "Gets the field at `ordinal` in the Array",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
            TypeSig.DECIMAL_64 + TypeSig.MAP).nested(),
        TypeSig.all,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_64 + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("ordinal", TypeSig.lit(TypeEnum.INT), TypeSig.INT)),
      (in, conf, p, r) => new GpuGetArrayItemMeta(in, conf, p, r){
        override def convertToGpu(arr: Expression, ordinal: Expression): GpuExpression =
          GpuGetArrayItem(arr, ordinal, SQLConf.get.ansiEnabled)
      }),
    GpuOverrides.expr[GetMapValue](
      "Gets Value from a Map based on a key",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.all,
        ("map", TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.all)),
        ("key", TypeSig.lit(TypeEnum.STRING), TypeSig.all)),
      (in, conf, p, r) => new GpuGetMapValueMeta(in, conf, p, r){
        override def convertToGpu(map: Expression, key: Expression): GpuExpression =
          GpuGetMapValue(map, key, SQLConf.get.ansiEnabled)
      }),
    GpuOverrides.expr[ElementAt](
      "Returns element of array at given(1-based) index in value if column is array. " +
          "Returns value for the given key in value if column is map.",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
            TypeSig.DECIMAL_64 + TypeSig.MAP).nested(), TypeSig.all,
        ("array/map", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_64 + TypeSig.MAP) +
            TypeSig.MAP.nested(TypeSig.STRING)
                .withPsNote(TypeEnum.MAP ,"If it's map, only string is supported."),
            TypeSig.ARRAY.nested(TypeSig.all) + TypeSig.MAP.nested(TypeSig.all)),
        ("index/key", (TypeSig.lit(TypeEnum.INT) + TypeSig.lit(TypeEnum.STRING))
            .withPsNote(TypeEnum.INT, "ints are only supported as array indexes, " +
                "not as maps keys")
            .withPsNote(TypeEnum.STRING, "strings are only supported as map keys, " +
                "not array indexes"),
            TypeSig.all)),
      (in, conf, p, r) => new BinaryExprMeta[ElementAt](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // To distinguish the supported nested type between Array and Map
          val checks = in.left.dataType match {
            case _: MapType =>
              // Match exactly with the checks for GetMapValue
              ExprChecks.binaryProject(TypeSig.STRING, TypeSig.all,
                ("map", TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.all)),
                ("key", TypeSig.lit(TypeEnum.STRING), TypeSig.all))
            case _: ArrayType =>
              // Match exactly with the checks for GetArrayItem
              ExprChecks.binaryProject(
                (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                    TypeSig.DECIMAL_64 + TypeSig.MAP).nested(),
                TypeSig.all,
                ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
                    TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_64 + TypeSig.MAP),
                    TypeSig.ARRAY.nested(TypeSig.all)),
                ("ordinal", TypeSig.lit(TypeEnum.INT), TypeSig.INT))
            case _ => throw new IllegalStateException("Only Array or Map is supported as input.")
          }
          checks.tag(this)
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuElementAt(lhs, rhs, SQLConf.get.ansiEnabled)
        }
      }),
    GpuOverrides.expr[Literal](
      "Holds a static value from the query",
      ExprChecks.projectAndAst(
        TypeSig.astTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64 + TypeSig.CALENDAR
          + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT + TypeSig.DAYTIME)
          .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64 +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
        TypeSig.all),
      (lit, conf, p, r) => new LiteralExprMeta(lit, conf, p, r)),
    GpuOverrides.expr[TimeAdd](
      "Adds interval to timestamp",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("start", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("interval", TypeSig.lit(TypeEnum.DAYTIME) + TypeSig.lit(TypeEnum.CALENDAR),
          TypeSig.DAYTIME + TypeSig.CALENDAR)),
      (timeAdd, conf, p, r) => new BinaryExprMeta[TimeAdd](timeAdd, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          GpuOverrides.extractLit(timeAdd.interval).foreach { lit =>
            lit.dataType match {
              case CalendarIntervalType =>
                val intvl = lit.value.asInstanceOf[CalendarInterval]
                if (intvl.months != 0) {
                  willNotWorkOnGpu("interval months isn't supported")
                }
              case _: DayTimeIntervalType => // Supported
            }
          }
          checkTimeZoneId(timeAdd.timeZoneId)
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuTimeAdd(lhs, rhs)
      }),
    GpuOverrides.expr[SpecifiedWindowFrame](
      "Specification of the width of the group (or \"frame\") of input rows " +
        "around which a window function is evaluated",
      ExprChecks.projectOnly(
        TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME,
        TypeSig.numericAndInterval,
        Seq(
          ParamCheck("lower",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME,
            TypeSig.numericAndInterval),
          ParamCheck("upper",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME,
            TypeSig.numericAndInterval))),
      (windowFrame, conf, p, r) => new GpuSpecifiedWindowFrameMeta(windowFrame, conf, p, r)),
    GpuOverrides.expr[WindowExpression](
      "Calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck("windowFunction",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all),
          ParamCheck("windowSpec",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_64 +
              TypeSig.DAYTIME, TypeSig.numericAndInterval))),
      (windowExpression, conf, p, r) => new GpuWindowExpressionMeta(windowExpression, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[WindowInPandasExec](
        "The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between" +
        " the Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled. For now it only supports row based window frame.",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested(TypeSig.commonCudfTypes),
          TypeSig.all),
        (winPy, conf, p, r) => new GpuWindowInPandasExecMetaBase(winPy, conf, p, r) {
          override val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
            winPy.windowExpression.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

          override def convertToGpu(): GpuExec = {
            GpuWindowInPandasExec(
              windowExpressions.map(_.convertToGpu()),
              partitionSpec.map(_.convertToGpu()),
              // leave ordering expression on the CPU, it's not used for GPU computation
              winPy.orderSpec,
              childPlans.head.convertIfNeeded()
            )(winPy.partitionSpec)
          }
        }).disabledByDefault("it only supports row based frame for now"),
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_64).nested(), TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)

          override def convertToGpu(): GpuExec = {
            val sparkSession = wrapped.relation.sparkSession
            val options = wrapped.relation.options

            val location = replaceWithAlluxioPathIfNeeded(
              conf,
              wrapped.relation,
              wrapped.partitionFilters,
              wrapped.dataFilters)

            val newRelation = HadoopFsRelation(
              location,
              wrapped.relation.partitionSchema,
              wrapped.relation.dataSchema,
              wrapped.relation.bucketSpec,
              GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
              options)(sparkSession)

            GpuFileSourceScanExec(
              newRelation,
              wrapped.output,
              wrapped.requiredSchema,
              wrapped.partitionFilters,
              wrapped.optionalBucketSet,
              wrapped.optionalNumCoalescedBuckets,
              wrapped.dataFilters,
              wrapped.tableIdentifier)(conf)
          }
        }),
      GpuOverrides.exec[InMemoryTableScanExec](
        "Implementation of InMemoryTableScanExec to use GPU accelerated Caching",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.STRUCT).nested(),
          TypeSig.all),
        (scan, conf, p, r) => new SparkPlanMeta[InMemoryTableScanExec](scan, conf, p, r) {
          override def tagPlanForGpu(): Unit = {
            if (!scan.relation.cacheBuilder.serializer.isInstanceOf[ParquetCachedBatchSerializer]) {
              willNotWorkOnGpu("ParquetCachedBatchSerializer is not being used")
            }
          }

          /**
           * Convert InMemoryTableScanExec to a GPU enabled version.
           */
          override def convertToGpu(): GpuExec = {
            GpuInMemoryTableScanExec(scan.attributes, scan.predicates, scan.relation)
          }
        }),
      GpuOverrides.exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ArrowEvalPythonExec](
        "The backend of the Scalar Pandas UDFs. Accelerates the data transfer between the" +
            " Java process and the Python process. It also supports scheduling GPU resources" +
            " for the Python process when enabled",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all),
        (e, conf, p, r) =>
          new SparkPlanMeta[ArrowEvalPythonExec](e, conf, p, r) {
            val udfs: Seq[BaseExprMeta[PythonUDF]] =
              e.udfs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
            val resultAttrs: Seq[BaseExprMeta[Attribute]] =
              e.resultAttrs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
            override val childExprs: Seq[BaseExprMeta[_]] = udfs ++ resultAttrs

            override def replaceMessage: String = "partially run on GPU"
            override def noReplacementPossibleMessage(reasons: String): String =
              s"cannot run even partially on the GPU because $reasons"

            override def convertToGpu(): GpuExec =
              GpuArrowEvalPythonExec(udfs.map(_.convertToGpu()).asInstanceOf[Seq[GpuPythonUDF]],
                resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
                childPlans.head.convertIfNeeded(),
                e.evalType)
          }),
      GpuOverrides.exec[MapInPandasExec](
        "The backend for Map Pandas Iterator UDF. Accelerates the data transfer between the" +
            " Java process and the Python process. It also supports scheduling GPU resources" +
            " for the Python process when enabled.",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all),
        (mapPy, conf, p, r) => new GpuMapInPandasExecMeta(mapPy, conf, p, r)),
      GpuOverrides.exec[FlatMapGroupsInPandasExec](
        "The backend for Flat Map Groups Pandas UDF, Accelerates the data transfer between the" +
            " Java process and the Python process. It also supports scheduling GPU resources" +
            " for the Python process when enabled.",
        ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
        (flatPy, conf, p, r) => new GpuFlatMapGroupsInPandasExecMeta(flatPy, conf, p, r)),
      GpuOverrides.exec[AggregateInPandasExec](
        "The backend for an Aggregation Pandas UDF, this accelerates the data transfer between" +
            " the Java process and the Python process. It also supports scheduling GPU resources" +
            " for the Python process when enabled.",
        ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
        (aggPy, conf, p, r) => new GpuAggregateInPandasExecMeta(aggPy, conf, p, r)),
      GpuOverrides.exec[BatchScanExec](
        "The backend for most file input",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.DECIMAL_64).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new SparkPlanMeta[BatchScanExec](p, conf, parent, r) {
          override val childScans: scala.Seq[ScanMeta[_]] =
            Seq(GpuOverrides.wrapScan(p.scan, conf, Some(this)))

          override def tagPlanForGpu(): Unit = {
            if (!p.runtimeFilters.isEmpty) {
              willNotWorkOnGpu("Runtime filtering (DPP) on datasource V2 is not supported")
            }
          }

          override def convertToGpu(): GpuExec =
            GpuBatchScanExec(p.output, childScans.head.convertToGpu())
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new ScanMeta[ParquetScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuParquetScanBase.tagSupport(this)
          // we are being overly cautious and that Parquet does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Parquet does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
        }

        override def convertToGpu(): Scan = {
          GpuParquetScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.pushedFilters,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf)
        }
      }),
    GpuOverrides.scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new ScanMeta[OrcScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuOrcScanBase.tagSupport(this)
          // we are being overly cautious and that Orc does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Orc does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
        }

        override def convertToGpu(): Scan =
          GpuOrcScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.pushedFilters,
            a.partitionFilters,
            a.dataFilters,
            conf)
      }),
    GpuOverrides.scan[CSVScan](
      "CSV parsing",
      (a, conf, p, r) => new ScanMeta[CSVScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuCSVScan.tagSupport(this)
          // we are being overly cautious and that Csv does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Csv does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
        }

        override def convertToGpu(): Scan =
          GpuCSVScan(a.sparkSession,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf.maxReadBatchSizeRows,
            conf.maxReadBatchSizeBytes)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  override def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  override def getPartitionFileNames(
      partitions: Seq[PartitionDirectory]): Seq[String] = {
    val files = partitions.flatMap(partition => partition.files)
    files.map(_.getPath.getName)
  }

  override def getPartitionFileStatusSize(partitions: Seq[PartitionDirectory]): Long = {
    partitions.map(_.files.map(_.getLen).sum).sum
  }

  override def getPartitionedFiles(
      partitions: Array[PartitionDirectory]): Array[PartitionedFile] = {
    partitions.flatMap { p =>
      p.files.map { f =>
        PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
      }
    }
  }

  override def getPartitionSplitFiles(
      partitions: Array[PartitionDirectory],
      maxSplitBytes: Long,
      relation: HadoopFsRelation): Array[PartitionedFile] = {
    partitions.flatMap { partition =>
      partition.files.flatMap { file =>
        // getPath() is very expensive so we only want to call it once in this block:
        val filePath = file.getPath
        val isSplitable = relation.fileFormat.isSplitable(
          relation.sparkSession, relation.options, filePath)
        PartitionedFileUtil.splitFiles(
          sparkSession = relation.sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }
    }
  }

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition]): RDD[InternalRow] = {
    new FileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def createFilePartition(index: Int, files: Array[PartitionedFile]): FilePartition = {
    FilePartition(index, files)
  }

  override def copyBatchScanExec(
      batchScanExec: GpuBatchScanExec,
      queryUsesInputFile: Boolean): GpuBatchScanExec = {
    val scanCopy = batchScanExec.scan match {
      case parquetScan: GpuParquetScan =>
        parquetScan.copy(queryUsesInputFile=queryUsesInputFile)
      case orcScan: GpuOrcScan =>
        orcScan.copy(queryUsesInputFile=queryUsesInputFile)
      case _ => throw new RuntimeException("Wrong format") // never reach here
    }
    batchScanExec.copy(scan=scanCopy)
  }

  override def copyFileSourceScanExec(
      scanExec: GpuFileSourceScanExec,
      queryUsesInputFile: Boolean): GpuFileSourceScanExec = {
    scanExec.copy(queryUsesInputFile=queryUsesInputFile)(scanExec.rapidsConf)
  }

  override def getGpuColumnarToRowTransition(plan: SparkPlan,
      exportColumnRdd: Boolean): GpuColumnarToRowExecParent = {
    val serName = plan.conf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
    val serClass = Class.forName(serName)
    if (serClass == classOf[ParquetCachedBatchSerializer]) {
      GpuColumnarToRowTransitionExec(plan)
    } else {
      GpuColumnarToRowExec(plan)
    }
  }

  override def checkColumnNameDuplication(
      schema: StructType,
      colType: String,
      resolver: Resolver): Unit = {
    GpuSchemaUtils.checkColumnNameDuplication(schema, colType, resolver)
  }

  override def getGpuShuffleExchangeExec(
      gpuOutputPartitioning: GpuPartitioning,
      child: SparkPlan,
      cpuOutputPartitioning: Partitioning,
      cpuShuffle: Option[ShuffleExchangeExec]): GpuShuffleExchangeExecBase = {
    val shuffleOrigin = cpuShuffle.map(_.shuffleOrigin).getOrElse(ENSURE_REQUIREMENTS)
    GpuShuffleExchangeExec(gpuOutputPartitioning, child, shuffleOrigin)(cpuOutputPartitioning)
  }

  override def getGpuShuffleExchangeExec(
      queryStage: ShuffleQueryStageExec): GpuShuffleExchangeExecBase = {
    queryStage.shuffle.asInstanceOf[GpuShuffleExchangeExecBase]
  }

  override def sortOrderChildren(s: SortOrder): Seq[Expression] = s.children

  override def sortOrder(
      child: Expression,
      direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder = SortOrder(child, direction, nullOrdering, Seq.empty)

  override def copySortOrderWithNewChild(s: SortOrder, child: Expression) = {
    s.copy(child = child)
  }

  override def alias(child: Expression, name: String)(
      exprId: ExprId,
      qualifier: Seq[String],
      explicitMetadata: Option[Metadata]): Alias = {
    Alias(child, name)(exprId, qualifier, explicitMetadata)
  }

  override def shouldIgnorePath(path: String): Boolean = {
    HadoopFSUtilsShim.shouldIgnorePath(path)
  }

  override def getLegacyComplexTypeToString(): Boolean = {
    SQLConf.get.getConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING)
  }

  // Arrow version changed between Spark versions
  override def getArrowDataBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getDataBuffer()
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

  override def getArrowValidityBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getValidityBuffer
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

  override def createTable(table: CatalogTable,
      sessionCatalog: SessionCatalog,
      tableLocation: Option[URI],
      result: BaseRelation) = {
    val newTable = table.copy(
      storage = table.storage.copy(locationUri = tableLocation),
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      schema = result.schema)
    // Table location is already validated. No need to check it again during table creation.
    sessionCatalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)
  }

  override def getArrowOffsetsBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getOffsetBuffer
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

  /** matches SPARK-33008 fix in 3.1.1 */
  override def shouldFailDivByZero(): Boolean = SQLConf.get.ansiEnabled

  override def replaceWithAlluxioPathIfNeeded(
      conf: RapidsConf,
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileIndex = {

    val alluxioPathsReplace: Option[Seq[String]] = conf.getAlluxioPathsToReplace

    if (alluxioPathsReplace.isDefined) {
      // alluxioPathsReplace: Seq("key->value", "key1->value1")
      // turn the rules to the Map with eg
      // { s3:/foo -> alluxio://0.1.2.3:19998/foo,
      //   gs:/bar -> alluxio://0.1.2.3:19998/bar,
      //   /baz -> alluxio://0.1.2.3:19998/baz }
      val replaceMapOption = alluxioPathsReplace.map(rules => {
        rules.map(rule => {
          val split = rule.split("->")
          if (split.size == 2) {
            split(0).trim -> split(1).trim
          } else {
            throw new IllegalArgumentException(s"Invalid setting for " +
                s"${RapidsConf.ALLUXIO_PATHS_REPLACE.key}")
          }
        }).toMap
      })

      replaceMapOption.map(replaceMap => {

        def isDynamicPruningFilter(e: Expression): Boolean =
          e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

        val partitionDirs = relation.location.listFiles(
          partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)

        // replacement func to check if the file path is prefixed with the string user configured
        // if yes, replace it
        val replaceFunc = (f: Path) => {
          val pathStr = f.toString
          val matchedSet = replaceMap.keySet.filter(reg => pathStr.startsWith(reg))
          if (matchedSet.size > 1) {
            // never reach here since replaceMap is a Map
            throw new IllegalArgumentException(s"Found ${matchedSet.size} same replacing rules " +
                s"from ${RapidsConf.ALLUXIO_PATHS_REPLACE.key} which requires only 1 rule " +
                s"for each file path")
          } else if (matchedSet.size == 1) {
            new Path(pathStr.replaceFirst(matchedSet.head, replaceMap(matchedSet.head)))
          } else {
            f
          }
        }

        // replace all of input files
        val inputFiles: Seq[Path] = partitionDirs.flatMap(partitionDir => {
          replacePartitionDirectoryFiles(partitionDir, replaceFunc)
        })

        // replace all of rootPaths which are already unique
        val rootPaths = relation.location.rootPaths.map(replaceFunc)

        val parameters: Map[String, String] = relation.options

        // infer PartitionSpec
        val partitionSpec = GpuPartitioningUtils.inferPartitioning(
          relation.sparkSession,
          rootPaths,
          inputFiles,
          parameters,
          Option(relation.dataSchema),
          replaceFunc)

        // generate a new InMemoryFileIndex holding paths with alluxio schema
        new InMemoryFileIndex(
          relation.sparkSession,
          inputFiles,
          parameters,
          Option(relation.dataSchema),
          userSpecifiedPartitionSpec = Some(partitionSpec))
      }).getOrElse(relation.location)

    } else {
      relation.location
    }
  }

  override def replacePartitionDirectoryFiles(partitionDir: PartitionDirectory,
      replaceFunc: Path => Path): Seq[Path] = {
    partitionDir.files.map(f => replaceFunc(f.getPath))
  }

  /**
   * Case class ShuffleQueryStageExec holds an additional field shuffleOrigin
   * affecting the unapply method signature
   */
  override def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec] = {
    case ShuffleQueryStageExec(_, e: ReusedExchangeExec, _) => e
    case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _) => e
  }

  /** dropped by SPARK-34234 */
  override def attachTreeIfSupported[TreeType <: TreeNode[_], A](
      tree: TreeType,
      msg: String)(
      f: => A
  ): A = {
    identity(f)
  }

  override def hasAliasQuoteFix: Boolean = true

  override def hasCastFloatTimestampUpcast: Boolean = true

  override def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan] = {
    def recurse(
        plan: SparkPlan,
        predicate: SparkPlan => Boolean,
        accum: ListBuffer[SparkPlan]): Seq[SparkPlan] = {
      if (predicate(plan)) {
        accum += plan
      }
      plan match {
        case a: AdaptiveSparkPlanExec => recurse(a.executedPlan, predicate, accum)
        case qs: BroadcastQueryStageExec => recurse(qs.broadcast, predicate, accum)
        case qs: ShuffleQueryStageExec => recurse(qs.shuffle, predicate, accum)
        case c: CommandResultExec => recurse(c.commandPhysicalPlan, predicate, accum)
        case other => other.children.flatMap(p => recurse(p, predicate, accum)).headOption
      }
      accum
    }
    recurse(plan, predicate, new ListBuffer[SparkPlan]())
  }

  override def skipAssertIsOnTheGpu(plan: SparkPlan): Boolean = plan match {
    case _: CommandResultExec => true
    case _ => false
  }

  override def registerKryoClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[SerializeConcatHostBuffersDeserializeBatch],
      new KryoJavaSerializer())
    kryo.register(classOf[SerializeBatchDeserializeHostBuffer],
      new KryoJavaSerializer())
  }

  override def getCentralMomentDivideByZeroEvalResult(): Expression = {
    val nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate
    GpuLiteral(if (nullOnDivideByZero) null else Double.NaN, DoubleType)
  }
}
