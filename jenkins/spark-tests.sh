#!/bin/bash
#
# Copyright (c) 2019-2021, NVIDIA CORPORATION. All rights reserved.
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
#

set -ex

nvidia-smi

. jenkins/version-def.sh

ARTF_ROOT="$WORKSPACE/jars"
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
    -Dmaven.repo.local=$WORKSPACE/.m2 \
    $MVN_URM_MIRROR -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT
# maven download SNAPSHOT jars: cudf, rapids-4-spark, spark3.0
$MVN_GET_CMD -DremoteRepositories=$CUDF_REPO \
    -DgroupId=ai.rapids -DartifactId=cudf -Dversion=$CUDF_VER -Dclassifier=$CUDA_CLASSIFIER
$MVN_GET_CMD -DremoteRepositories=$PROJECT_REPO \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark_$SCALA_BINARY_VER -Dversion=$PROJECT_VER
$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-udf-examples_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER

# TODO remove -Dtransitive=false workaround once pom is fixed
$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -Dtransitive=false \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER -Dclassifier=$SHUFFLE_SPARK_SHIM
if [ "$CUDA_CLASSIFIER"x == x ];then
    CUDF_JAR="$ARTF_ROOT/cudf-$CUDF_VER.jar"
else
    CUDF_JAR="$ARTF_ROOT/cudf-$CUDF_VER-$CUDA_CLASSIFIER.jar"
fi
export RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-$PROJECT_VER.jar"
RAPIDS_UDF_JAR="$ARTF_ROOT/rapids-4-spark-udf-examples_${SCALA_BINARY_VER}-$PROJECT_TEST_VER.jar"
RAPIDS_TEST_JAR="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER-$SHUFFLE_SPARK_SHIM.jar"

# TODO remove -Dtransitive=false workaround once pom is fixed
$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -Dtransitive=false \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER -Dclassifier=pytest -Dpackaging=tar.gz

RAPIDS_INT_TESTS_HOME="$ARTF_ROOT/integration_tests/"
# The version of pytest.tar.gz that is uploaded is the one built against spark301 but its being pushed without classifier for now
RAPIDS_INT_TESTS_TGZ="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER-pytest.tar.gz"

tmp_info=${TMP_INFO_FILE:-'/tmp/artifacts-build.info'}
rm -rf "$tmp_info"
TEE_CMD="tee -a $tmp_info"
GREP_CMD="grep revision"
AWK_CMD=(awk -F'=' '{print $2}')
getRevision() {
  local file=$1
  local properties=$2
  local revision
  if [[ $file == *.jar || $file == *.zip ]]; then
    revision=$(unzip -p "$file" "$properties" | $TEE_CMD | $GREP_CMD | "${AWK_CMD[@]}" || true)
  elif [[ $file == *.tgz || $file == *.tar.gz ]]; then
    revision=$(tar -xzf "$file" --to-command=cat "$properties" | $TEE_CMD | $GREP_CMD | "${AWK_CMD[@]}" || true)
  fi
  echo "$revision"
}

set +x
echo -e "\n==================== ARTIFACTS BUILD INFO ====================\n" >> "$tmp_info"
echo "-------------------- cudf JNI BUILD INFO --------------------" >> "$tmp_info"
c_ver=$(getRevision $JARS_PATH/$CUDF_JAR cudf-java-version-info.properties)
echo "-------------------- rapids-4-spark BUILD INFO --------------------" >> "$tmp_info"
p_ver=$(getRevision $JARS_PATH/$RAPIDS_PLUGIN_JAR rapids4spark-version-info.properties)
echo "-------------------- rapids-4-spark-integration-tests BUILD INFO --------------------" >> "$tmp_info"
it_ver=$(getRevision $JARS_PATH/$RAPIDS_TEST_JAR rapids4spark-version-info.properties)
echo "-------------------- rapids-4-spark-integration-tests pytest BUILD INFO --------------------" >> "$tmp_info"
pt_ver=$(getRevision $JARS_PATH/$RAPIDS_INT_TESTS_TGZ integration_tests/rapids4spark-version-info.properties)
echo "-------------------- rapids-4-spark-udf-examples BUILD INFO --------------------" >> "$tmp_info"
u_ver=$(getRevision $JARS_PATH/$RAPIDS_UDF_JAR rapids4spark-version-info.properties)
echo -e "\n==================== ARTIFACTS BUILD INFO ====================\n" >> "$tmp_info"
set -x
cat "$tmp_info" || true

SKIP_REVISION_CHECK=${SKIP_REVISION_CHECK:-'false'}
if [[ "$SKIP_REVISION_CHECK" != "true" && (-z "$c_ver" || -z "$p_ver"|| \
      "$p_ver" != "$it_ver" || "$p_ver" != "$pt_ver" || "$p_ver" != "$u_ver") ]]; then
  echo "Artifacts revisions are inconsistent!"
  exit 1
fi

tar xzf "$RAPIDS_INT_TESTS_TGZ" -C $ARTF_ROOT && rm -f "$RAPIDS_INT_TESTS_TGZ"

$MVN_GET_CMD -DremoteRepositories=$SPARK_REPO \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3.2 -Dpackaging=tgz

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3.2"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz

IS_SPARK_311_OR_LATER=0
[[ "$(printf '%s\n' "3.1.1" "$SPARK_VER" | sort -V | head -n1)" = "3.1.1" ]] && IS_SPARK_311_OR_LATER=1

export SPARK_TASK_MAXFAILURES=1
[[ "$IS_SPARK_311_OR_LATER" -eq "0" ]] && SPARK_TASK_MAXFAILURES=4

export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
# enable worker cleanup to avoid "out of space" issue
# if failed, we abort the test instantly, so the failed executor log should still be left there for debugging
export SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS -Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=120 -Dspark.worker.cleanup.appDataTtl=60"
#stop and restart SPARK ETL
stop-slave.sh
stop-master.sh
start-master.sh
start-slave.sh spark://$HOSTNAME:7077
jps

echo "----------------------------START TEST------------------------------------"
pushd $RAPIDS_INT_TESTS_HOME

export BASE_SPARK_SUBMIT_ARGS="$BASE_SPARK_SUBMIT_ARGS \
--master spark://$HOSTNAME:7077 \
--conf spark.sql.shuffle.partitions=12 \
--conf spark.task.maxFailures=$SPARK_TASK_MAXFAILURES \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
--conf spark.sql.session.timeZone=UTC"

export SEQ_CONF="--executor-memory 16G \
--total-executor-cores 6"

export PARALLEL_CONF="--executor-memory 4G \
--total-executor-cores 1 \
--conf spark.executor.cores=1 \
--conf spark.task.cpus=1 \
--conf spark.rapids.sql.concurrentGpuTasks=1 \
--conf spark.rapids.memory.gpu.minAllocFraction=0"

export CUDF_UDF_TEST_ARGS="--conf spark.rapids.memory.gpu.allocFraction=0.1 \
--conf spark.rapids.memory.gpu.minAllocFraction=0 \
--conf spark.rapids.python.memory.gpu.allocFraction=0.1 \
--conf spark.rapids.python.concurrentPythonWorkers=2 \
--conf spark.executorEnv.PYTHONPATH=${RAPIDS_PLUGIN_JAR} \
--conf spark.pyspark.python=/opt/conda/bin/python \
--py-files ${RAPIDS_PLUGIN_JAR}"

export TEST_PARALLEL=0  # disable spark local parallel in run_pyspark_from_build.sh
export TEST_TYPE="nightly"
export LOCAL_JAR_PATH=$ARTF_ROOT
export SCRIPT_PATH="$(pwd -P)"
export TARGET_DIR="$SCRIPT_PATH/target"
mkdir -p $TARGET_DIR

run_test_not_parallel() {
    local TEST=${1//\.py/}
    local LOG_FILE
    case $TEST in
      all)
        SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $SEQ_CONF" \
          ./run_pyspark_from_build.sh
        ;;

      cudf_udf_test)
        SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $SEQ_CONF $CUDF_UDF_TEST_ARGS" \
          ./run_pyspark_from_build.sh -m cudf_udf --cudf_udf
        ;;

      cache_serializer)
        SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $SEQ_CONF \
        --conf spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer" \
          ./run_pyspark_from_build.sh -k cache_test
        ;;

      *)
        echo -e "\n\n>>>>> $TEST...\n"
        LOG_FILE="$TARGET_DIR/$TEST.log"
        # set dedicated RUN_DIRs here to avoid conflict between parallel tests
        RUN_DIR="$TARGET_DIR/run_dir_$TEST" \
          SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $PARALLEL_CONF $MEMORY_FRACTION_CONF" \
          ./run_pyspark_from_build.sh -k $TEST >"$LOG_FILE" 2>&1

        CODE="$?"
        if [[ $CODE == "0" ]]; then
          sed -n -e '/test session starts/,/deselected,/ p' "$LOG_FILE" || true
        else
          cat "$LOG_FILE" || true
          cat /tmp/artifacts-build.info || true
        fi
        return $CODE
        ;;
    esac
}
export -f run_test_not_parallel

# TEST_MODE
# - IT_ONLY
# - CUDF_UDF_ONLY
# - ALL: IT+CUDF_UDF
TEST_MODE=${TEST_MODE:-'IT_ONLY'}
if [[ $TEST_MODE == "ALL" || $TEST_MODE == "IT_ONLY" ]]; then
  # integration tests
  if [[ $PARALLEL_TEST == "true" ]] && [ -x "$(command -v parallel)" ]; then
    # We separate tests/cases into different categories for parallel run to try avoid long tail distribution
    # time_consuming_tests: tests that would cost over 1 hour if run sequentially, we split them into cases (time_consuming_tests_cases)
    # mem_consuming_cases: cases in time_consuming_tests that would consume much more GPU memory than normal cases
    # other_tests: tests except time_consuming_tests_cases and mem_consuming_cases

    # TODO: Tag these tests/cases
    # time-consuming tests, space-separated
    time_consuming_tests="join_test hash_aggregate_test generate_expr_test parquet_write_test orc_test orc_write_test"
    # GPU memory-consuming cases in time_consuming_tests, space-separated
    mem_consuming_cases="test_hash_reduction_decimal_overflow_sum"
    # hardcode parallelism as 2 for gpu-mem consuming cases
    export MEMORY_FRACTION_CONF="--conf spark.rapids.memory.gpu.allocFraction=0.45 \
    --conf spark.rapids.memory.gpu.maxAllocFraction=0.45"
    # --halt "now,fail=1": exit when the first job fail, and kill running jobs.
    #                      we can set it to "never" and print failed ones after finish running all tests if needed
    # --group: print stderr after test finished for better readability
    parallel --group --halt "now,fail=1" -j2 run_test_not_parallel ::: ${mem_consuming_cases}

    time_consuming_tests_str=$(echo ${time_consuming_tests} | xargs | sed 's/ / or /g')
    mem_consuming_cases_str=$(echo ${mem_consuming_cases} | xargs | sed 's/ / and not /g')
    time_consuming_tests_cases=$(./run_pyspark_from_build.sh -k \
                                 "(${time_consuming_tests_str}) and not ${mem_consuming_cases_str}" \
                                  --collect-only -qq 2>/dev/null | grep -oP '(?<=::).*?(?=\[)' | uniq | shuf | xargs)
    other_tests=$(./run_pyspark_from_build.sh --collect-only -qqq 2>/dev/null | grep -oP '(?<=python/).*?(?=.py)' \
                  | grep -vP "$(echo ${time_consuming_tests} | xargs | tr ' ' '|')")
    tests=$(echo "${time_consuming_tests_cases} ${other_tests}" | tr ' ' '\n' | awk '!x[$0]++' | xargs)

    if [[ "${PARALLELISM}" == "" ]]; then
      PARALLELISM=$(nvidia-smi --query-gpu=memory.free --format=csv,noheader | \
                    awk '{if (MAX < $1){ MAX = $1}} END {print int(MAX / (2 * 1024))}')
    fi
    MEMORY_FRACTION=$(python -c "print(1/($PARALLELISM + 0.1))")
    export MEMORY_FRACTION_CONF="--conf spark.rapids.memory.gpu.allocFraction=${MEMORY_FRACTION} \
    --conf spark.rapids.memory.gpu.maxAllocFraction=${MEMORY_FRACTION}"
    parallel --group --halt "now,fail=1" -j"${PARALLELISM}" run_test_not_parallel ::: $tests
  else
    run_test_not_parallel all
  fi

  if [[ "$IS_SPARK_311_OR_LATER" -eq "1" ]]; then
    if [[ $PARALLEL_TEST == "true" ]] && [ -x "$(command -v parallel)" ]; then
      cache_test_cases=$(./run_pyspark_from_build.sh -k "cache_test" \
                            --collect-only -qq 2>/dev/null | grep -oP '(?<=::).*?(?=\[)' | uniq | shuf | xargs)
      # hardcode parallelism as 5
      export MEMORY_FRACTION_CONF="--conf spark.rapids.memory.gpu.allocFraction=0.18 \
      --conf spark.rapids.memory.gpu.maxAllocFraction=0.18 \
      --conf spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer"
      parallel --group --halt "now,fail=1" -j5 run_test_not_parallel ::: ${cache_test_cases}
    else
      run_test_not_parallel cache_serializer
    fi
  fi
fi

# cudf_udf_test
if [[ "$TEST_MODE" == "ALL" || "$TEST_MODE" == "CUDF_UDF_ONLY" ]]; then
  run_test_not_parallel cudf_udf_test
fi

popd
stop-slave.sh
stop-master.sh
