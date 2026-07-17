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

# Get current date in YYYYMMDD format
$time = Get-Date -Format "yyyyMMdd"

# Pull commit from iotdb
$IoTDB_BASE_BRANCH = "master"
# Create branch based on IoTDB_BASE_BRANCH
$IoTDB_COMMIT_BRANCH = "iotdb_${IoTDB_BASE_BRANCH}"
# Pull commit from timechodb
$TIMECHODB_BASE_BRANCH = "master"
# Create mr branch based on TIMECHODB_BASE_BRANCH
$LOCAL_MR_BRANCH = "sync_from_iotdb_${IoTDB_BASE_BRANCH}_${time}"
$PRIVATE_COMMIT_PREFIX = "TIMECHODB"

function setup_env {
    $HAS_MASTER_IOTDB = git branch -vv | Select-String -Pattern "\s${IoTDB_COMMIT_BRANCH}\s"
    if (-not $HAS_MASTER_IOTDB) {
        Write-Host "Create branch ${IoTDB_COMMIT_BRANCH}"
        git remote add iotdb git@gitlab-eco.timecho.com:r-d/hybrid-db/iotdb.git
        git fetch --quiet iotdb $IoTDB_BASE_BRANCH
        git checkout --quiet FETCH_HEAD
        git checkout -b $IoTDB_COMMIT_BRANCH
    } else {
        git checkout $IoTDB_COMMIT_BRANCH
    }
    Write-Host "Pull iotdb ${IoTDB_BASE_BRANCH}"
    git pull --quiet iotdb $IoTDB_BASE_BRANCH

    $HAS_MASTER = git branch -vv | Select-String -Pattern "\s${LOCAL_MR_BRANCH}\s"
    if (-not $HAS_MASTER) {
        Write-Host "Create branch ${LOCAL_MR_BRANCH}"
        git fetch --quiet origin $TIMECHODB_BASE_BRANCH
        git checkout --quiet FETCH_HEAD
        git checkout -b $LOCAL_MR_BRANCH
    } else {
        git checkout $LOCAL_MR_BRANCH
        Write-Host "Pull origin ${TIMECHODB_BASE_BRANCH}"
        git pull --quiet origin $TIMECHODB_BASE_BRANCH
    }
}

function cherry_pick {
    param([string]$CommitHash)
    git cherry-pick $CommitHash
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Please resolve conflicts manually."
        exit 1
    }
}

setup_env

# Find the last community commit
$LAST_COMMUNITY_COMMIT_MSG = (git log --pretty=format:"%s" | Where-Object { $_ -notmatch "(?i)${PRIVATE_COMMIT_PREFIX}" -and $_ -notmatch "Merge branch" }) | Select-Object -First 1

if (-not $LAST_COMMUNITY_COMMIT_MSG) {
    Write-Host "Failed to find the last community commit"
    exit 1
}

Write-Host "Last community commit: ${LAST_COMMUNITY_COMMIT_MSG}"

# Get the hash of the last community commit from IoTDB_COMMIT_BRANCH
$LAST_COMMUNITY_COMMIT_HASH = (git log --pretty=format:"%h %s" $IoTDB_COMMIT_BRANCH | Select-String -SimpleMatch $LAST_COMMUNITY_COMMIT_MSG | ForEach-Object { ($_ -split ' ', 2)[0] } | Select-Object -First 1)

if (-not $LAST_COMMUNITY_COMMIT_HASH) {
    Write-Host "Failed to find the hash of the last community commit"
    exit 1
}

Write-Host "Last community commit hash: ${LAST_COMMUNITY_COMMIT_HASH}"

# Cherry-pick new commits one by one
$ALL_HASHES = git log --pretty=format:"%h" $IoTDB_COMMIT_BRANCH
$idx = [array]::IndexOf($ALL_HASHES, $LAST_COMMUNITY_COMMIT_HASH)
$NUM_COMMITS = if ($idx -ge 0) { $idx } else { 0 }
Write-Host "Found ${NUM_COMMITS} commit(s) to cherry-pick"
Write-Host ""

$COMMIT_HASHES = if ($idx -ge 0) { $ALL_HASHES[0..($idx - 1)] } else { @() }
$COMMIT_HASHES = $COMMIT_HASHES[$($COMMIT_HASHES.Length - 1)..0]

foreach ($COMMIT_HASH in $COMMIT_HASHES) {
    $ONELINE = git show --oneline --quiet $COMMIT_HASH
    Write-Host "Working on:"
    Write-Host $ONELINE

    do {
        $CMD = Read-Host "Input command (summary/detail/cp/skip)"
        switch ($CMD.ToLower()) {
            "summary" { git show --stat $COMMIT_HASH }
            "detail" { git show $COMMIT_HASH }
            "cp" { cherry_pick $COMMIT_HASH }
            "skip" { }
            default { Write-Host "Invalid command" }
        }
    } until ($CMD -in @("cp", "skip"))

    Write-Host ""
}
