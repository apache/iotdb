#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

time=$(date "+%Y%m%d")

# pull commit from iotdb
IoTDB_BASE_BRANCH="master"
# create brahch base on IoTDB_BASE_BRANCH
IoTDB_COMMIT_BRANCH="iotdb_${IoTDB_BASE_BRANCH}"

# pull commit from timechodb
TIMECHODB_BASE_BRANCH="master"
# create mr branch base on TIMECHODB_BASE_BRANCH
LOCAL_MR_BRANCH="sync_from_iotdb_${IoTDB_BASE_BRANCH}_${time}"

PRIVATE_COMMIT_PREFIX="TIMECHODB"

function setup_env() {
 HAS_MASTER_IOTDB=$(git branch -vv | grep -w "${IoTDB_COMMIT_BRANCH}")
 if [ -z "${HAS_MASTER_IOTDB}" ]
 then
  echo "Create branch ${IoTDB_COMMIT_BRANCH}"
  git remote add iotdb git@gitlab-eco.timecho.com:r-d/hybrid-db/iotdb.git
  git fetch --quiet iotdb ${IoTDB_BASE_BRANCH}
  git checkout --quiet FETCH_HEAD
  git checkout -b ${IoTDB_COMMIT_BRANCH}
 else
  git checkout ${IoTDB_COMMIT_BRANCH}
 fi
 echo "Pull iotdb ${IoTDB_BASE_BRANCH}"
 git pull --quiet iotdb ${IoTDB_BASE_BRANCH}

 HAS_MASTER=$(git branch -vv | grep -w "${LOCAL_MR_BRANCH}")
 if [ -z "${HAS_MASTER}" ]
 then
  echo "Create branch ${LOCAL_MR_BRANCH}"
  git fetch --quiet origin "${TIMECHODB_BASE_BRANCH}"
  git checkout --quiet FETCH_HEAD
  git checkout -b ${LOCAL_MR_BRANCH}
 else
  git checkout ${LOCAL_MR_BRANCH}
  echo "Pull origin ${TIMECHODB_BASE_BRANCH}"
  git pull --quiet origin ${TIMECHODB_BASE_BRANCH}
 fi
}

function cherry_pick() {
 git cherry-pick $1
 if [ $? -ne 0 ]
 then
     echo "Please resolve conflicts manually."
     exit 1
 fi
}

setup_env

# Find the last community commit
LAST_COMMUNITY_COMMIT_MSG=$(git log --pretty=format:"%s" | grep -i -v "${PRIVATE_COMMIT_PREFIX}" |grep -v "Merge branch" | head -1)
if [ -z "${LAST_COMMUNITY_COMMIT_MSG}" ]
then
 echo "Failed to find the last community commit"
 exit 1
fi
echo "Last community commit: ${LAST_COMMUNITY_COMMIT_MSG}"

LAST_COMMUNITY_COMMIT_HASH=$(git log --pretty=format:"%h %s" ${IoTDB_COMMIT_BRANCH} | grep -F "${LAST_COMMUNITY_COMMIT_MSG}" | awk '{print $1}' | head -1)
if [ -z "${LAST_COMMUNITY_COMMIT_HASH}" ]
then
 echo "Failed to find the hash of the last community commit"
 exit 1
fi
echo "Last community commit hash: ${LAST_COMMUNITY_COMMIT_HASH}"

# Cherry-pick new commits one by one
NUM_COMMITS=$(git log --pretty=format:"%h" ${IoTDB_COMMIT_BRANCH} | grep -B 10000 "${LAST_COMMUNITY_COMMIT_HASH}" | grep -v "${LAST_COMMUNITY_COMMIT_HASH}" | wc -l)
echo "Found ${NUM_COMMITS} commit(s) to cherry-pick"
echo ""

for COMMIT_HASH in $(git log --pretty=format:"%h" ${IoTDB_COMMIT_BRANCH} | grep -B 10000 "${LAST_COMMUNITY_COMMIT_HASH}" | grep -v "${LAST_COMMUNITY_COMMIT_HASH}" | tac)
do
 echo "Working on:"
 echo $(git show --oneline --quiet ${COMMIT_HASH})
 while true; do
  read -p "  Input command (summary/detail/cp/skip): " CMD
  case ${CMD} in
   summary ) git show --stat ${COMMIT_HASH};;
            detail  ) git show ${COMMIT_HASH};;
   cp      ) cherry_pick ${COMMIT_HASH}; break;;
            skip    ) break;;
   *       ) echo "  Invalid command";;
  esac
 done
 echo ""
done
