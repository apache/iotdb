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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BFSAccessStrategy implements AccessStrategy {

  Queue<Object> sameLevelMemNodes;

  @Override
  public <I, O, C extends Context> void execute(
      BasicLevelProcess<I, O, C> levelProcess, I memNode, C context) {
    List<O> children = new ArrayList<>();
    int currentLevel = context.getLevel();
    // 第一个使用bfs策略的节点
    if (sameLevelMemNodes == null) {
      sameLevelMemNodes = new LinkedList<>();
      levelProcess.handle(memNode, context);
      children = levelProcess.getChildren(memNode, context);
    } else {
      while (!sameLevelMemNodes.isEmpty()) {
        I node = (I) sameLevelMemNodes.poll();
        levelProcess.handle(node, context);
        children.addAll(levelProcess.getChildren(node, context));
      }
    }
    sameLevelMemNodes.addAll(children);
    context.setLevel(currentLevel + 1);
    if (levelProcess.hasNext() && !sameLevelMemNodes.isEmpty()) {
      levelProcess.getNext().process(null, context);
    }
  }
}
