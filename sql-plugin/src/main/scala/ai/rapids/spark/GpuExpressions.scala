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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BinaryOp, BinaryOperable, ColumnVector, DType, Scalar, UnaryOp}
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BinaryOperator, Expression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.vectorized.ColumnarBatch


object GpuExpressionsUtils {


  def evaluateBoundExpressions[A <: GpuExpression](cb: ColumnarBatch, boundExprs: Seq[A]) : Seq[GpuColumnVector] = {
    val numCols = boundExprs.length
    val resultCvs = new ArrayBuffer[GpuColumnVector](numCols)
    try {
      for (i <- 0 until numCols) {
        val ref = boundExprs(i)
        resultCvs += ref.columnarEval(cb).asInstanceOf[GpuColumnVector]
      }
    } catch {
      case t: Throwable =>
        resultCvs.safeClose()
        throw t
    }
    resultCvs
  }
}

/**
 * An Expression that cannot be evaluated in the traditional row-by-row sense (hence Unevaluable)
 * but instead can be evaluated on an entire column batch at once.
 */
trait GpuExpression extends Expression with Unevaluable {
  /**
   * Returns the result of evaluating this expression on the entire
   * [[ColumnarBatch]]. The result of calling this may be a single [[GpuColumnVector]] or a scalar
   * value. Scalar values typically happen if they are a part of the expression i.e. col("a") + 100.
   * In this case the 100 is a literal that Add would have to be able to handle.
   *
   * By convention any [[GpuColumnVector]] returned by [[columnarEval]]
   * is owned by the caller and will need to be closed by them. This can happen by putting it into
   * a [[ColumnarBatch]] and closing the batch or by closing the vector directly if it is a
   * temporary value.
   */
  def columnarEval(batch: ColumnarBatch): Any
}

abstract class GpuLeafExpression extends GpuExpression {
  override final def children: Seq[Expression] = Nil
}

trait GpuUnevaluable extends GpuExpression {
  final override def columnarEval(batch: ColumnarBatch): Any =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

abstract class GpuUnevaluableUnaryExpression extends GpuUnaryExpression with GpuUnevaluable {
  final override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

abstract class GpuUnaryExpression extends UnaryExpression with GpuExpression {
  protected def doColumnar(input: GpuColumnVector): GpuColumnVector

  def outputTypeOverride: DType = null

  override def columnarEval(batch: ColumnarBatch): Any = {
    val input = child.asInstanceOf[GpuExpression].columnarEval(batch)
    try {
      input match {
        case vec: GpuColumnVector =>
          var tmp = doColumnar(vec)
          try {
            val base = tmp.getBase
            if (outputTypeOverride != null && outputTypeOverride != base.getType) {
              GpuColumnVector.from(base.castTo(outputTypeOverride))
            } else {
              val r = tmp
              tmp = null
              r
            }
          } finally {
            if (tmp != null) {
              tmp.close()
            }
          }
        case _ => throw new IllegalStateException(
          s"Unary expression $this should only see a column result from child eval")
      }
    } finally {
      if (input.isInstanceOf[AutoCloseable]) {
        input.asInstanceOf[AutoCloseable].close()
      }
    }
  }
}

trait CudfUnaryExpression extends GpuUnaryExpression {
  def unaryOp: UnaryOp

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.unaryOp(unaryOp))
}

trait GpuBinaryExpression extends BinaryExpression with GpuExpression {

  def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector
  def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector
  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    try {
      lhs = left.asInstanceOf[GpuExpression].columnarEval(batch)
      rhs = right.asInstanceOf[GpuExpression].columnarEval(batch)

      (lhs, rhs) match {
        case (l: GpuColumnVector, r: GpuColumnVector) => doColumnar(l, r)
        case (l, r: GpuColumnVector) =>
          val scalar = GpuScalar.from(l, left.dataType)
          try {
            doColumnar(scalar, r)
          } finally {
            scalar.close()
          }
        case (l: GpuColumnVector, r) =>
          val scalar = GpuScalar.from(r, right.dataType)
          try {
            doColumnar(l, scalar)
          } finally {
            scalar.close()
          }
        case (l, r) if (l != null && r != null) => nullSafeEval(l, r)
        case _ => null
      }
    } finally {
      if (lhs.isInstanceOf[AutoCloseable]) {
        lhs.asInstanceOf[AutoCloseable].close()
      }
      if (rhs.isInstanceOf[AutoCloseable]) {
        rhs.asInstanceOf[AutoCloseable].close()
      }
    }
  }

  def createAllTrueColumnVector(lhs: GpuColumnVector) : GpuColumnVector = {
    var ret : GpuColumnVector = null
    var b : ColumnVector.Builder = null
      try {
        b = ColumnVector.builder(DType.BOOL8, lhs.getRowCount.toInt)
        (0 until lhs.getRowCount.toInt).foreach(i => b.append(true))
        ret = GpuColumnVector.from(b.build())
      } finally {
      b.close()
    }
    ret
  }
}

trait GpuBinaryOperator extends BinaryOperator with GpuBinaryExpression

trait CudfBinaryExpression extends GpuBinaryExpression {
  def binaryOp: BinaryOp
  def outputTypeOverride: DType = null

  def outputType(l: BinaryOperable, r: BinaryOperable) : DType = {
    val over = outputTypeOverride
    if (over == null) {
      BinaryOperable.implicitConversion(l, r)
    } else {
      over
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    val lBase = lhs.getBase
    val rBase = rhs.getBase
    val outType = outputType(lBase, rBase)
    GpuColumnVector.from(lBase.binaryOp(binaryOp, rBase, outType))
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    val rBase = rhs.getBase
    val outType = outputType(lhs, rBase)
    GpuColumnVector.from(lhs.binaryOp(binaryOp, rBase, outType))
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    val lBase = lhs.getBase
    val outType = outputType(lBase, rhs)
    GpuColumnVector.from(lBase.binaryOp(binaryOp, rhs, outType))
  }
}

abstract class CudfBinaryOperator extends GpuBinaryOperator with CudfBinaryExpression
