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

public enum CompactionTaskStage {
  NOT_STARTED("task does not started"),
  STARTED("task has started"),
  TARGET_FILE_GENERATED("target file has been generated completely"),
  TARGET_FILE_REPLACED("target file has been replaced in TsFileManager"),
  SOURCE_FILE_CLEANED("source files are cleaned up"),
  FINISHED("task finished");

  private final String descirption;

  CompactionTaskStage(String descirption) {
    this.descirption = descirption;
  }

  public String getDescription() {
    return this.descirption;
  }
}
