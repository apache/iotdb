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

package org.apache.iotdb.db.engine.tieredstorage.config;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tieredstorage.task.TsFileMigrationTriggerType;

/**
 * // (jinrui)
 * 该类为一个单例类，用来在运行时提供多级存储的相关配置（可能需要定义更多的类来实现它），其功能为：
 * 1. 启动时，根据配置文件处理多级存储的配置信息
 * 2. 将配置信息存储在该单例类的对象中，并提供对外的访问接口
 * 3. 提供判断一个 TsFile 是否需要迁移的方法，并返回迁移的原因
 */
public class TieredStorageConfig {

  // (jinrui) 传入参数可能需要更多信息，以辅助判断，尽可能地使用内存中的信息进行该判断，避免磁盘读取
  // 如果在实现过程中需要从磁盘中读取信息，需要标注出来进行讨论
  public TsFileMigrationTriggerType needMoveToNextTier(TsFileResource tsFileResource) {
    return null;
  }
}
