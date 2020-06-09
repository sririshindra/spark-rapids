/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.RegExpReplace
import org.apache.spark.sql.rapids.GpuStringReplace

class StringFallbackSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual(
    "String regexp_replace replace str columnar fall back",
    nullableStringsFromCsv,
    execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result = frame.selectExpr("regexp_replace(strings,'a',strings)")
      checkPlanForRegexpReplace(result)
      result
  }

  testSparkResultsAreEqual("String regexp_replace null cpu fall back",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result = frame.selectExpr("regexp_replace(strings,null,'D')")
      checkPlanForRegexpReplace(result)
      result
  }

  testSparkResultsAreEqual("String regexp_replace input empty cpu fall back",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result = frame.selectExpr("regexp_replace(strings,'','D')")
      checkPlanForRegexpReplace(result)
      result
  }

  testSparkResultsAreEqual("String regexp_replace regex 1 cpu fall back",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result = frame.selectExpr("regexp_replace(strings,'.*','D')")
      checkPlanForRegexpReplace(result)
      result
  }

  testSparkResultsAreEqual("String regexp_replace regex 2 cpu fall back",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result =  frame.selectExpr("regexp_replace(strings,'[a-z]+','D')")
      checkPlanForRegexpReplace(result)
      result
  }

  testSparkResultsAreEqual("String regexp_replace regex 3 cpu fall back",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result = frame.selectExpr("regexp_replace(strings,'foo$','D')")
      checkPlanForRegexpReplace(result)
      result
  }

  testSparkResultsAreEqual("String regexp_replace regex 4 cpu fall back",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result = frame.selectExpr("regexp_replace(strings,'^foo','D')")
      checkPlanForRegexpReplace(result)
      result
  }

  testSparkResultsAreEqual("String regexp_replace regex 5 cpu fall back",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result = frame.selectExpr("regexp_replace(strings,'(foo)','D')")
      checkPlanForRegexpReplace(result)
      result
  }

  testSparkResultsAreEqual("String regexp_replace regex 6 cpu fall back",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => val result = frame.selectExpr("regexp_replace(strings,'\\(foo\\)','D')")
      checkPlanForRegexpReplace(result)
      result
  }

  private def checkPlanForRegexpReplace(result: DataFrame): Unit = {
    // Asserts that RegexpReplace fell back to the CPU and and was not replaced by GpuStringReplace
    val executedPlan = result.queryExecution.executedPlan
    val childExprs = executedPlan.flatMap(_.expressions).flatMap(_.children)
    assert(!childExprs.exists(_.isInstanceOf[GpuStringReplace]))
    assert(childExprs.exists(_.isInstanceOf[RegExpReplace]))
    //extra cautious and make sure project is on the CPU in any case
    assert(executedPlan.find(_.isInstanceOf[GpuProjectExec]).isEmpty)
  }

}
