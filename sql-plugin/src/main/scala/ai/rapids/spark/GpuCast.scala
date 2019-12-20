/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{DType, Scalar}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.types._

object GpuCast {
  private val MICROS_PER_SEC_DOUBLE = Scalar.fromDouble(1000000)
  private val MICROS_PER_SEC_INT = Scalar.fromInt(1000000)

  /**
   * Returns true iff we can cast `from` to `to` using the GPU.
   *
   * Eventually we will need to match what is supported by spark proper
   * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala#L37-L95
   */
  def canCast(from: DataType, to: DataType): Boolean =
    (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (BooleanType, _: NumericType) => true

    case (_: NumericType, BooleanType) => true
    case (_: NumericType, _: NumericType) => true

    case (DateType, BooleanType) => true
    case (DateType, _: NumericType) => true
    case (DateType, TimestampType) => true

    case (TimestampType, BooleanType) => true
    case (TimestampType, _: NumericType) => true
    case (TimestampType, DateType) => true

    case _ => false
  }
}

/**
 * Casts using the GPU
 */
case class GpuCast(child: GpuExpression, dataType: DataType, timeZoneId: Option[String] = None)
  extends GpuUnaryExpression with TimeZoneAwareExpression with NullIntolerant {

  override def toString: String = s"cast($child as ${dataType.simpleString})"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (Cast.canCast(child.dataType, dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"cannot cast ${child.dataType.catalogString} to ${dataType.catalogString}")
    }
  }

  override def nullable: Boolean = Cast.forceNullable(child.dataType, dataType) || child.nullable

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  // When this cast involves TimeZone, it's only resolved if the timeZoneId is set;
  // Otherwise behave like Expression.resolved.
  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && (!needsTimeZone || timeZoneId.isDefined)

  private[this] def needsTimeZone: Boolean = Cast.needsTimeZone(child.dataType, dataType)

  override def sql: String = dataType match {
    // HiveQL doesn't allow casting to complex types. For logical plans translated from HiveQL, this
    // type of casting can only be introduced by the analyzer, and can be omitted when converting
    // back to SQL query string.
    case _: ArrayType | _: MapType | _: StructType => child.sql
    case _ => s"CAST(${child.sql} AS ${dataType.sql})"
  }

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    val cudfType = GpuColumnVector.getRapidsType(dataType)

    (input.dataType(), dataType) match {
      case (_: NumericType | TimestampType, BooleanType) =>
        // normally a straightforward cast could be used, but due to the problem reported in
        // https://github.com/rapidsai/cudf/issues/2575 a workaround is used here.
        GpuColumnVector.from(input.getBase.notEqualTo(Scalar.fromLong(0L)))
      case (DateType, BooleanType | _: NumericType) =>
        // casts from date type to numerics are always null
        GpuColumnVector.from(GpuScalar.from(null, dataType), input.getBase.getRowCount.toInt)
      case (TimestampType, FloatType|DoubleType) =>
        // Use trueDiv to ensure cast to double before division for full precision
        GpuColumnVector.from(input.getBase.trueDiv(GpuCast.MICROS_PER_SEC_DOUBLE, cudfType))
      case (TimestampType, ByteType | ShortType | IntegerType) =>
        // normally we would just do a floordiv here, but cudf downcasts the operands to
        // the output type before the divide.  https://github.com/rapidsai/cudf/issues/2574
        val cv = input.getBase.floorDiv(GpuCast.MICROS_PER_SEC_INT, DType.INT64)
        try {
          GpuColumnVector.from(cv.castTo(cudfType))
        } finally {
          cv.close()
        }
      case (TimestampType, _: NumericType) =>
        GpuColumnVector.from(input.getBase.floorDiv(GpuCast.MICROS_PER_SEC_INT, cudfType))
      case _ =>
        GpuColumnVector.from(input.getBase.castTo(cudfType))
    }
  }
}
