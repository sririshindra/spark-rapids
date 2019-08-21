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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.{DataFrame, RandomDataGenerator, Row, SparkSession}
import org.apache.spark.sql.types._

class SortExecSuite extends SparkQueryCompareTestSuite {

  // For sort we want to make sure duplicates so when sort on both columns
  // sorting happens properly. We also want nulls to make sure null handling correct
  def nullableLongsDfWithDuplicates(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Long, java.lang.Long)](
      (100L, 1L),
      (200L, null),
      (300L, 3L),
      (800L, 3L),
      (400L, 4L),
      (null, 4L),
      (null, 5L),
      (-100L, 6L),
      (null, 0L)
    ).toDF("longs", "more_longs")
  }

  def generateData(dataType: DataType, nullable: Boolean, size: Int): (SparkSession => DataFrame) = {
    val generator = RandomDataGenerator.forType(dataType, nullable).get
    val inputData = Seq.fill(size)(generator())
    (session: SparkSession) => {
      import session.sqlContext.implicits._
      session.createDataFrame(
        session.sparkContext.parallelize(Random.shuffle(inputData).map(v => Row(v))),
        StructType(StructField("a", dataType, nullable = true) :: Nil)
      )
    }
  }

  // Note I -- out the set of Types that aren't supported with Sort right now so we can explicitly see them and remove
  // individually as we add support
  for (
    dataType <- DataTypeTestUtils.atomicTypes ++ Set(NullType) -- Set(FloatType, StringType, NullType, DoubleType, DecimalType.USER_DEFAULT,
      DecimalType(20, 5), DecimalType.SYSTEM_DEFAULT, BinaryType);
    nullable <- Seq(true, false);
    sortOrder <- Seq(col("a").asc, col("a").asc_nulls_last, col("a").desc, col("a").desc_nulls_first)
  ) {
    val inputDf = generateData(dataType, nullable, 10)
    testSparkResultsAreEqual(s"sorting on $dataType with nullable=$nullable, sortOrder=$sortOrder",  inputDf, allowNonGpu=false, execsAllowedNonGpu = Seq("RDDScanExec", "AttributeReference")) {
      frame => frame.sortWithinPartitions(sortOrder)
    }
  }

  testSparkResultsAreEqual("sort 2 cols longs nulls", nullableLongsDfWithDuplicates) {
    frame => frame.sortWithinPartitions("longs", "more_longs")
  }

  testSparkResultsAreEqual("sort 2 cols longs expr", longsDf) {
    frame => frame.sortWithinPartitions(col("longs") + 1, col("more_longs"))
  }

  testSparkResultsAreEqual("sort 2 cols longs nulls desc/desc", nullableLongsDfWithDuplicates) {
    frame => frame.sortWithinPartitions(col("longs").desc, col("more_longs").desc)
  }

  testSparkResultsAreEqual("sort 2 cols longs nulls last desc/desc", nullableLongsDfWithDuplicates) {
    frame => frame.sortWithinPartitions(col("longs").desc_nulls_last, col("more_longs").desc_nulls_last)
  }

  // force a sortMergeJoin
  private val sortJoinConf = new SparkConf().set("spark.sql.autoBroadcastJoinThreshold", "-1").
    set("spark.sql.join.preferSortMergeJoin", "true").set("spark.sql.exchange.reuse", "false")

  testSparkResultsAreEqual2("join longs", longsDf, longsDf, conf = sortJoinConf,
      allowNonGpu = true, sort = true) {
    (dfA, dfB) => dfA.join(dfB, dfA("longs") === dfB("longs"))
  }

  private val sortJoinMultiBatchConf = sortJoinConf.set("spark.sql.inMemoryColumnarStorage.batchSize", "3")

  testSparkResultsAreEqual2("join longs multiple batches", longsDf, longsDf,
      conf = sortJoinMultiBatchConf, allowNonGpu = true, sort = true) {
    (dfA, dfB) => dfA.join(dfB, dfA("longs") === dfB("longs"))
  }
}
