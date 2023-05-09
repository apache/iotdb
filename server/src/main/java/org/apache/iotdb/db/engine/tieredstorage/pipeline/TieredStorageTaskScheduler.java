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

package org.apache.iotdb.db.engine.tieredstorage.pipeline;

import org.apache.iotdb.db.engine.storagegroup.TsFileManager;

/**
 * // (jinrui) 取一个什么名字好呢？ 负责周期性地检查是否需要提交 TsFileMigrationTask，初步计划每个 DataRegion
 * 有一个这样的实例，定期执行；DataRegion 之间不混用
 */
public class TieredStorageTaskScheduler {

  private String dataRegionId;
  // (jinrui)
  // 获取一个 TsFileManager 的引用，从而拿到对应的文件信息
  // 相应的，系统重启的时候，在温数据区加载文件时，也要做相应的改动（读对应的 resource 文件）
  private TsFileManager tsFileManager;

  public TieredStorageTaskScheduler(String dataRegionId) {
    this.dataRegionId = dataRegionId;
  }

  public void schedule() {}

  private void checkTsFileTTL() {}

  // (jinrui)
  // 这里涉及到了一个问题，就是当磁盘超过阈值时，迁移文件时候怎么进行选择，有两个倾向
  // 1. 从一个 DataRegion 迁移，直至预期空间小于阈值为止
  // 2. 每个 DataRegion 迁移最老的 N 个文件，然后等待下一轮再进行检查
  private void checkDiskThreshold() {}
}
