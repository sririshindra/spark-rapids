#
# Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
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

# This script analyzes the hashCommitsWithMessages.log generated by scripts/audit-spark*.sh
# and determines which commits are directly modifying classes that the plugin depends upon i.e. to 
# be more explicit, which classes have jdeps determined as a dependency for the plugin. Commits
# that are deemed to have touched upon classes that the rapids-4-spark plugin is dependent on will 
# will be flagged as P1, all the other commits will be flagged as P3. 
# The output of this script will be a file in $WORKSPACE called commits-with-priority.txt which 
# will have the output in the following format 
# <Priority (P1 or P?)> <COMMIT-HASH> <COMMIT-MSG>
# in this the COMMIT-HASH and COMMIT-MSG are the original sha1 and commit message from spark commit

#!/bin/bash

set -ex

#Set env variables
. jenkins/version-def.sh

ABSOLUTE_PATH=$(cd $(dirname $0) && pwd)
AUDIT_PLUGIN_LOG=${ABSOLUTE_PATH}/audit-plugin.log
if [ -e ${AUDIT_PLUGIN_LOG}]; then 
  rm ${AUDIT_PLUGIN_LOG}
fi

#Get plugin jar
ARTF_ROOT="$WORKSPACE/jars"
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
    -Dmaven.repo.local=$WORKSPACE/.m2 \
    -DrepoUrl=https://urm.nvidia.com/artifactory/sw-spark-maven -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT
# maven download SNAPSHOT jars: rapids-4-spark, spark3.0
$MVN_GET_CMD -DremoteRepositories=$PROJECT_REPO \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark_$SCALA_BINARY_VER -Dversion=$PROJECT_VER

RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-$PROJECT_VER.jar"
# Use jdeps to find the dependencies of the rapids-4-spark jar
DEPS=$(jdeps -include "(com\.nvidia\.spark.*|org\.apache\.spark.*)" -v -e org.apache.spark.* $RAPIDS_PLUGIN_JAR)

FOUND=1
cd ${SPARK_TREE}
PRIORITIZED_COMMITS=${WORKSPACE}/commits-with-priority.txt
if [ -e ${PRIORITIZED_COMMITS} ]; then
  rm ${PRIORITIZED_COMMITS}
fi  
# Read each commit and commit message
while read COMMIT_ID COMMIT_MSG
do
  echo checking commit $COMMIT_ID
  # Get the scala and java files changed in this commit
  set +ex
  GIT_NAME_ONLY=$(git show $COMMIT_ID --name-only --oneline | grep '.*scala\|java')
  set -ex
  CLASSES=""
  for i in $GIT_NAME_ONLY
  do
    # Get the classes from the each file. This is tricky and is only relying on picking out the 
    # object/trait/class from the git show command. It then tries to figure out the fully-qualified 
    # class name from the file name and store them in LOCAL_CLASSES. We can make this regex better to 
    # focus on only code changes.
    set +ex
    LOCAL_CLASSES=$(git show $COMMIT_ID -- $i | grep '\sclass\s\|\sobject\s\|\strait\s|\sinterface\s'|sed 's/.*trait \([[:alnum:]]*\).*/\1/' | sed 's/.*class \([[:alnum:]]*\).*/\1/' | sed 's/.*object \([[:alnum:]]*\).*/\1/')
    set -ex
    # Figure out the package from the file name
    PACKAGE=$(dirname -- $i | sed 's/.*scala\///g' | sed 's/.*java\///g' | sed 's/\//\./g')
    for CLASS in $LOCAL_CLASSES
    do
      # Prepend the package to the class name
      CLASSES="${CLASSES}\n$PACKAGE.${CLASS}"
    done
  done
  # dedupe
  for CLASS in $(echo -e $CLASSES|sort|uniq|xargs)
  do
  if [ ! -z "${CLASS}" ]; then
    # Look for the class name in the dependencies to determine if this change affects us
    set +ex
    FOUND=$(grep -c ${CLASS} <<< ${DEPS})
    set -ex
    echo -e "looking for class ${CLASS} and ${FOUND}\n"
    if [ ${FOUND} -ne 0 ]; then 
      printf "%s\t%s\t%s\t%s\n" "P1" "${COMMIT_ID}" "${COMMIT_MSG}" "${CLASS}" >> ${PRIORITIZED_COMMITS}
      printf "%s\t%s\n" "${COMMIT_ID}" "${CLASS}" >> ${AUDIT_PLUGIN_LOG}
      break
    fi
  fi
  done
  if [ ${FOUND} -eq 0 ]; then
    printf "%s\t%s\t%s\n" "P?" "${COMMIT_ID}" "${COMMIT_MSG}" >> ${PRIORITIZED_COMMITS}
  fi
done < <(cat ${COMMIT_DIFF_LOG})

