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

package org.apache.iotdb.db.engine.tieredstorage.task;

import org.apache.iotdb.db.engine.tieredstorage.exception.TsFileMigrationException;

import java.util.List;

public abstract class TsFileMigrationTask {
  // block size for each write when doing migration to avoid memory overflow
  private static final int BLOCK_SIZE_PER_WRITE = 100 * 1024 * 1024;
  private String sourceFilePath;
  private String targetFilePath;
  private TsFileMigrationTriggerType triggerType;
  private TsFileMigrationTaskType taskType;

  // (jinrui) 文件的输入都是从本地读，所以大概率是用同一个文件读取类即可，不过也可以抽象一个读取的接口

  // (jinrui) 定义一个输出文件的接口，不同的 MigrationTask 提供不同的输出文件接口的实现，需要确认当前写 TsFile 是用的什么接口
  // private XXFileWriter writer;

  public TsFileMigrationTask(
      String sourceFilePath,
      String targetFilePath,
      TsFileMigrationTriggerType triggerType,
      TsFileMigrationTaskType taskType) {
    this.sourceFilePath = sourceFilePath;
    this.targetFilePath = targetFilePath;
    this.triggerType = triggerType;
    this.taskType = taskType;
  }

  /**
   * // (jinrui) 该方法实现迁移一个 TsFile 的完整步骤： 1. 根据 getSourceFileList() 获取到所有要迁移的文件 2. 按照文件列表中的顺序，使用对应的
   * reader 和 writer 对文件进行读写 3. 如果迁移过程中遇到了异常，则执行 cleanupWhenException()
   */
  public void doMigration() throws TsFileMigrationException {}

  // (jinrui) 不同的迁移任务它涉及的文件不同，比如 local_to_local 不仅需要迁移 source file，还需要迁移对应的 mod、resource 文件
  protected abstract List<String> getSourceFileList();

  /** (jinrui) 迁移任务如果遇到异常，则需要进行相应的清理工作。可以考虑是否需要将该方式实现成一个统一的方法 */
  protected abstract void cleanupWhenException() throws TsFileMigrationException;
}
