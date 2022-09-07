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
package org.apache.iotdb.lsm.strategy;

import org.apache.iotdb.lsm.context.Context;
import org.apache.iotdb.lsm.levelProcess.BasicLevelProcess;

// 表示内存节点访问策略（先序，后序）
public interface AccessStrategy {

  /**
   * @param levelProcess 保存当前节点和子节点的处理方法
   * @param memNode 当前待处理的节点
   * @param context 上下文信息
   */
  <I, O, C extends Context> void execute(
      BasicLevelProcess<I, O, C> levelProcess, I memNode, C context);
}
