/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log;

import org.apache.iotdb.db.i18n.StorageEngineMessages;

public enum CompactionTaskStage {
  NOT_STARTED(StorageEngineMessages.EXCEPTION_TASK_DOES_NOT_STARTED_B7A8C59D),
  STARTED(StorageEngineMessages.EXCEPTION_TASK_HAS_STARTED_8F884490),
  TARGET_FILE_GENERATED(
      StorageEngineMessages.EXCEPTION_TARGET_FILE_HAS_BEEN_GENERATED_COMPLETELY_DBDCCC37),
  TARGET_FILE_REPLACED(
      StorageEngineMessages.EXCEPTION_TARGET_FILE_HAS_BEEN_REPLACED_IN_TSFILEMANAGER_9456BDD6),
  SOURCE_FILE_CLEANED(StorageEngineMessages.EXCEPTION_SOURCE_FILES_ARE_CLEANED_UP_83638D68),
  FINISHED(StorageEngineMessages.EXCEPTION_TASK_FINISHED_A075ACB1);

  private final String descirption;

  CompactionTaskStage(String descirption) {
    this.descirption = descirption;
  }

  public String getDescription() {
    return this.descirption;
  }
}
