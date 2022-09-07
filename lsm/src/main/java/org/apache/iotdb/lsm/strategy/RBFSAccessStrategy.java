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
import org.apache.iotdb.lsm.context.FlushContext;
import org.apache.iotdb.lsm.levelProcess.BasicLevelProcess;

import java.util.List;

public class RBFSAccessStrategy implements AccessStrategy {
  @Override
  public <I, O, C extends Context> void execute(
      BasicLevelProcess<I, O, C> levelProcess, I memNode, C context) {
    FlushContext flushContext = (FlushContext) context;
    int currentLevel = context.getLevel();
    // 如果当前节点是最深的一层节点
    if (!levelProcess.hasNext()) {
      flushContext.setMinimumFlushedLevel(currentLevel);
    }
    // 如果是根节点
    if (currentLevel == 0) {
      while (flushContext.getMinimumFlushedLevel() != currentLevel) {
        List<O> children = levelProcess.getChildren(memNode, context);
        for (O child : children) {
          // 处理子节点
          flushContext.setLevel(currentLevel + 1);
          levelProcess.getNext().process(child, context);
          flushContext.setLevel(currentLevel);
        }
        // 每次处理完-1
        flushContext.setMinimumFlushedLevel(flushContext.getMinimumFlushedLevel() - 1);
      }
      // 处理root节点
      levelProcess.handle(memNode, context);
      return;
    }

    // 已经处理过，直接return
    if (currentLevel > flushContext.getMinimumFlushedLevel()) return;

    // 处理子节点
    if (currentLevel == flushContext.getMinimumFlushedLevel()) {
      levelProcess.handle(memNode, context);
      return;
    }
    List<O> children = levelProcess.getChildren(memNode, context);
    for (O child : children) {
      flushContext.setLevel(currentLevel + 1);
      levelProcess.getNext().process(child, context);
      flushContext.setLevel(currentLevel);
    }
  }
}
