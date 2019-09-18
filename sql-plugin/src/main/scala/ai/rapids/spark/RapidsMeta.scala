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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.connector.read.Scan

trait ConfKeysAndIncompat {
  val operationName: String
  def incompatDoc: Option[String] = None

  def confKey: String
}

/**
 * A version of ConfKeysAndIncompat that is used when no replacement rule can be found.
 */
final class NoRuleConfKeysAndIncompat extends ConfKeysAndIncompat {
  override val operationName: String = "NOT_FOUND"

  override def confKey = "NOT_FOUND"
}

/**
 * Holds metadata about a stage in the physical plan that is separate from the plan itself.
 * This is helpful in deciding when to replace part of the plan with a GPU enabled version.
 *
 * @param wrapped what we are wrapping
 * @param conf the config
 * @param parent the parent of this node, if there is one.
 * @param rule holds information related to the config for this object, typically this is the rule
 *          used to wrap the stage.
 * @tparam INPUT the exact type of the class we are wrapping.
 * @tparam BASE the generic base class for this type of stage, i.e. SparkPlan, Expression, etc.
 * @tparam OUTPUT when converting to a GPU enabled version of the plan, the generic base
 *                    type for all GPU enabled versions.
 */
abstract class RapidsMeta[INPUT <: BASE, BASE, OUTPUT <: BASE](
    val wrapped: INPUT,
    val conf: RapidsConf,
    val parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat) {

  /**
   * The wrapped plans that should be examined
   */
  val childPlans: Seq[SparkPlanMeta[_]]

  /**
   * The wrapped expressions that should be examined
   */
  val childExprs: Seq[ExprMeta[_]]

  /**
   * The wrapped scans that should be examined
   */
  val childScans: Seq[ScanMeta[_]]

  /**
   * The wrapped partitioning that should be examined
   */
  val childParts: Seq[PartMeta[_]]

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  def convertToGpu(): OUTPUT

  /**
   * Keep this on the CPU, but possibly convert its children under it to run on the GPU if enabled.
   * By default this just returns what is wrapped by this.  For some types of operators/stages,
   * like SparkPlan, each part of the query can be converted independent of other parts. As such in
   * a subclass this should be overridden to do the correct thing.
   */
  def convertToCpu(): BASE = wrapped

  private var cannotBeReplacedReasons: Option[mutable.Set[String]] = None

  private var shouldBeRemovedReasons: Option[mutable.Set[String]] = None

  /**
   * Call this to indicate that this should not be replaced with a GPU enabled version
   * @param because why it should not be replaced.
   */
  final def willNotWorkOnGpu(because: String): Unit =
    cannotBeReplacedReasons.get.add(because)

  final def shouldBeRemoved(because: String): Unit =
    shouldBeRemovedReasons.get.add(because)

  def isParentInstanceOf[A]: Boolean = parent.exists(_.wrapped.isInstanceOf[A])

  /**
   * Returns true if this node should be removed.
   */
  final def shouldThisBeRemoved: Boolean = shouldBeRemovedReasons.exists(_.nonEmpty)

  /**
   * Returns true iff this could be replaced.
   */
  final def canThisBeReplaced: Boolean = cannotBeReplacedReasons.exists(_.isEmpty)

  /**
   * Returns true iff all of the expressions and their children could be replaced.
   */
  def canExprTreeBeReplaced: Boolean = childExprs.forall(_.canExprTreeBeReplaced)

  /**
   * Returns true iff all of the scans can be replaced.
   */
  def canScansBeReplaced: Boolean = childScans.forall(_.canThisBeReplaced)

  /**
   * Returns true iff all of the partitioning can be replaced.
   */
  def canPartsBeReplaced: Boolean = childParts.forall(_.canThisBeReplaced)

  def confKey: String = rule.confKey
  final val operationName: String = rule.operationName
  final val incompatDoc: Option[String] = rule.incompatDoc
  def isIncompat: Boolean = incompatDoc.isDefined

  def initReasons(): Unit = {
    cannotBeReplacedReasons = Some(mutable.Set[String]())
    shouldBeRemovedReasons = Some(mutable.Set[String]())
  }

  /**
   * Tag all of the children to see if they are GPU compatible first.
   * Do basic common verification for the operators, and then call
   * [[tagSelfForGpu]]
   */
  final def tagForGpu(): Unit = {
    childScans.foreach(_.tagForGpu())
    childParts.foreach(_.tagForGpu())
    childExprs.foreach(_.tagForGpu())
    childPlans.foreach(_.tagForGpu())

    initReasons()

    if (!conf.isOperatorEnabled(confKey, isIncompat)) {
      if (isIncompat && !conf.isIncompatEnabled) {
        willNotWorkOnGpu(s"the GPU version of ${wrapped.getClass.getSimpleName}" +
          s" is not 100% compatible with the Spark version. ${incompatDoc.get}. To enable this" +
          s" ${operationName} despite the incompatibilities please set the config" +
          s" ${confKey} to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
          s" to enable all incompatible ops")
      } else {
        willNotWorkOnGpu(s"the ${operationName} ${wrapped.getClass.getSimpleName} has" +
          s" been disabled. Set ${confKey} to true if you wish to enable it")
      }
    }

    tagSelfForGpu()
  }

  /**
   * Do any extra checks and tag yourself if you are compatible or not.  Be aware that this may
   * already have been marked as incompatible for a number of reasons.
   *
   * All of your children should have already been tagged so if there are situations where you
   * may need to disqualify your children for various reasons you may do it here too.
   */
  def tagSelfForGpu(): Unit

  private def indent(append: StringBuilder, depth: Int): Unit =
    append.append("  " * depth)

  private def willWorkOnGpuInfo: String = cannotBeReplacedReasons match {
    case None => "NOT EVALUATED FOR GPU YET"
    case Some(v) if v.isEmpty => "could run on GPU"
    case Some(v) =>
      val reasons = v mkString "; "
      s"cannot run on GPU because ${reasons}"
  }

  private def willBeRemovedInfo: String = shouldBeRemovedReasons match {
    case None => ""
    case Some(v) if v.isEmpty => ""
    case Some(v) =>
      val reasons = v mkString "; "
      s" but is going to be removed because ${reasons}"
  }

  /**
   * When converting this to a string should we include the string representation of what this
   * wraps too?  This is off by default.
   */
  protected val printWrapped = false

  /**
   * Create a string representation of this in append.
   * @param append where to place the string representation.
   * @param depth how far down the tree this is.
   */
  protected def print(strBuilder: StringBuilder, depth: Int): Unit = {
    indent(strBuilder, depth)
    strBuilder.append(if (canThisBeReplaced) "*" else "!")

    strBuilder.append(operationName)
      .append(" <")
      .append(wrapped.getClass.getSimpleName)
      .append("> ")

    if (printWrapped) {
      strBuilder.append(wrapped)
        .append(" ")
    }

    strBuilder.append(willWorkOnGpuInfo).
      append(willBeRemovedInfo).
      append("\n")

    printChildren(strBuilder, depth)
  }

  private final def printChildren(append: StringBuilder, depth: Int): Unit = {
    childScans.foreach(_.print(append, depth + 1))
    childParts.foreach(_.print(append, depth + 1))
    childExprs.foreach(_.print(append, depth + 1))
    childPlans.foreach(_.print(append, depth + 1))
  }

  override def toString: String = {
    val appender = new StringBuilder()
    print(appender, 0)
    appender.toString()
  }
}


/**
 * Base class for metadata around [[Partitioning]].
 */
abstract class PartMeta[INPUT <: Partitioning](part: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends RapidsMeta[INPUT, Partitioning, GpuPartitioning](part, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[ExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty

  override final def tagSelfForGpu(): Unit = {
    if (!canExprTreeBeReplaced) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }
    tagPartForGpu()
  }

  def tagPartForGpu(): Unit = {}
}

/**
 * Metadata for [[Partitioning]] with no rule found
 */
final class RuleNotFoundPartMeta[INPUT <: Partitioning](
    part: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends PartMeta[INPUT](part, conf, parent, new NoRuleConfKeysAndIncompat) {

  override def tagPartForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU enabled version of partitioning ${part.getClass} could be found")
  }

  override def convertToGpu(): GpuPartitioning =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around [[Scan]].
 */
abstract class ScanMeta[INPUT <: Scan](scan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends RapidsMeta[INPUT, Scan, Scan](scan, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[ExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty

  override def tagSelfForGpu(): Unit = {}
}

/**
 * Metadata for [[Scan]] with no rule found
 */
final class RuleNotFoundScanMeta[INPUT <: Scan](
    scan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends ScanMeta[INPUT](scan, conf, parent, new NoRuleConfKeysAndIncompat) {

  override def tagSelfForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU enabled version of scan ${scan.getClass} could be found")
  }

  override def convertToGpu(): Scan =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around [[SparkPlan]].
 */
abstract class SparkPlanMeta[INPUT <: SparkPlan](plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends RapidsMeta[INPUT, SparkPlan, GpuExec](plan, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = plan.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
  override val childExprs: Seq[ExprMeta[_]] = plan.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty

  override def convertToCpu(): SparkPlan = {
    wrapped.withNewChildren(childPlans.map(_.convertIfNeeded()))
  }

  private def findShuffleExchanges(): Seq[SparkPlanMeta[ShuffleExchangeExec]] = wrapped match {
    case _: ShuffleExchangeExec =>
      this.asInstanceOf[SparkPlanMeta[ShuffleExchangeExec]] :: Nil
    case bkj: BroadcastHashJoinExec => bkj.buildSide match {
      case BuildLeft => childPlans(1).findShuffleExchanges()
      case BuildRight => childPlans(0).findShuffleExchanges()
    }
    case _ => childPlans.flatMap(_.findShuffleExchanges())
  }

  private def makeShuffleConsistent(): Unit = {
    val exchanges = findShuffleExchanges()
    if (!exchanges.forall(_.canThisBeReplaced)) {
      exchanges.foreach(_.willNotWorkOnGpu("other exchanges that feed the same join are" +
        " on the CPU and GPU hashing is not consistent with the CPU version"))
    }
  }

  private def fixUpJoinConsistencyIfNeeded(): Unit = wrapped match {
    case _: ShuffledHashJoinExec => makeShuffleConsistent()
    case _: SortMergeJoinExec => makeShuffleConsistent()
    case _ => ()
  }

  override final def tagSelfForGpu(): Unit = {
    if (!GpuOverrides.areAllSupportedTypes(plan.output.map(_.dataType) :_*)) {
      val unsupported = plan.output.map(_.dataType).filter(!GpuOverrides.areAllSupportedTypes(_)).toSet
      willNotWorkOnGpu(s"unsupported data types in output: ${unsupported.mkString(", ")}")
    }
    if (!GpuOverrides.areAllSupportedTypes(plan.children.flatMap(_.output.map(_.dataType)) :_*)) {
      val unsupported = plan.children.flatMap(_.output.map(_.dataType))
        .filter(!GpuOverrides.areAllSupportedTypes(_)).toSet
      willNotWorkOnGpu(s"unsupported data types in input: ${unsupported.mkString(", ")}")
    }

    if (!canExprTreeBeReplaced) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }

    if (!canScansBeReplaced) {
      willNotWorkOnGpu("not all scans can be replaced")
    }

    if (!canPartsBeReplaced) {
      willNotWorkOnGpu("not all partitioning can be replaced")
    }

    tagPlanForGpu()

    // In general children will be tagged first and then their parents will be tagged.  This gives
    // flexibility when tagging yourself to look at your children and disable yourself if your
    // children are not all on the GPU.  But in some cases we need to be able to disable our
    // children too.  These exceptions should be documented here.  We need to take special care
    // that we take into account all side-effects of these changes, because we are **not**
    // re-triggering the rules associated with parents, grandparents, etc.  If things get too
    // complicated we may need to update this to have something with triggers, but then we would
    // have to be very careful to avoid loops in the rules.
    // RULES:
    // 1) BroadcastHashJoin can disable the Broadcast directly feeding it, if the join itself cannot
    // be translated for some reason.  This is okay because it is the joins immediate parent, so
    // it can keep everything consistent.
    // 2) For ShuffledHashJoin and SortMergeJoin we need to verify that all of the exchanges feeding
    // them are either all on the GPU or all on the CPU, because the hashing is not consistent
    // between the two implementations. This is okay because it is only impacting shuffled exchanges.
    // So broadcast exchanges are not impacted which could have an impact on BroadcastHashJoin, and
    // shuffled exchanges are not used to disable anything downstream.
    fixUpJoinConsistencyIfNeeded()
  }

  /**
   * Called to verify that this plan will work on the GPU. Generic checks will have already been
   * done. In general this method should only tag this operator as bad.  If it needs to tag
   * one of its children please take special care to update the comment inside [[tagSelfForGpu()]]
   * so we don't end up with something that could be cyclical.
   */
  def tagPlanForGpu(): Unit = {}

  /**
   * If this is enabled to be converted to a GPU version convert it and return the result, else
   * do what is needed to possibly convert the rest of the plan.
   */
  final def convertIfNeeded(): SparkPlan = {
    if (shouldThisBeRemoved) {
      if (childPlans.isEmpty) {
        throw new IllegalStateException("can't remove when plan has no children")
      } else if (childPlans.size > 1) {
        throw new IllegalStateException("can't remove when plan has more than 1 child")
      }
      childPlans(0).convertIfNeeded()
    } else {
      if (canThisBeReplaced) {
        convertToGpu()
      } else {
        convertToCpu()
      }
    }
  }
}

/**
 * Metadata for [[SparkPlan]] with no rule found
 */
final class RuleNotFoundSparkPlanMeta[INPUT <: SparkPlan](
    plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends SparkPlanMeta[INPUT](plan, conf, parent, new NoRuleConfKeysAndIncompat) {

  override def tagPlanForGpu(): Unit =
    willNotWorkOnGpu(s"no GPU enabled version of operator ${plan.getClass} could be found")

  override def convertToGpu(): GpuExec =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around [[Expression]].
 */
abstract class ExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends RapidsMeta[INPUT, Expression, GpuExpression](expr, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[ExprMeta[_]] = expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty

  override val printWrapped: Boolean = true

  override def canExprTreeBeReplaced: Boolean =
    canThisBeReplaced && super.canExprTreeBeReplaced

  final override def tagSelfForGpu(): Unit = {
    if (!GpuOverrides.areAllSupportedTypes(expr.dataType)) {
      willNotWorkOnGpu(s"expression ${expr.getClass.getSimpleName} ${expr} " +
        s"produces an unsupported type ${expr.dataType}")
    }
    tagExprForGpu()
  }

  /**
   * Called to verify that this expression will work on the GPU. For most expressions without
   * extra checks all of the checks should have already been done.
   */
  def tagExprForGpu(): Unit = {}
}

/**
 * Base class for metadata around [[UnaryExpression]].
 */
abstract class UnaryExprMeta[INPUT <: UnaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs(0).convertToGpu())

  def convertToGpu(child: GpuExpression): GpuExpression
}

/**
 * Base class for metadata around [[AggregateFunction]].
 */
abstract class AggExprMeta[INPUT <: AggregateFunction](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs(0).convertToGpu())

  def convertToGpu(child: GpuExpression): GpuExpression
}

/**
 * Base class for metadata around [[BinaryExpression]].
 */
abstract class BinaryExprMeta[INPUT <: BinaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs(0).convertToGpu(), childExprs(1).convertToGpu())

  def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression
}

/**
 * Metadata for [[Expression]] with no rule found
 */
final class RuleNotFoundExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends ExprMeta[INPUT](expr, conf, parent, new NoRuleConfKeysAndIncompat) {

  override def tagExprForGpu(): Unit =
    willNotWorkOnGpu(s"no GPU enabled version of expression ${expr.getClass} could be found")

  override def convertToGpu(): GpuExpression =
    throw new IllegalStateException("Cannot be converted to GPU")
}
