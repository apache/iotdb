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

import org.apache.iotdb.lsm.context.requestcontext.RequestContext;
import org.apache.iotdb.lsm.levelProcess.BasicLevelProcessor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/** breadth-first access strategy implementation class */
public class BFSAccessStrategy implements IAccessStrategy {

  // same level memory nodes, used to implement BFSAccessStrategy
  Queue<Object> sameLevelMemNodes;

  /**
   * breadth-first access strategy implementation
   *
   * @param levelProcess current level process
   * @param memNode memory node
   * @param context request context
   */
  @Override
  public <I, O, R, C extends RequestContext> void execute(
      BasicLevelProcessor<I, O, R, C> levelProcess, I memNode, R request, C context) {
    List<O> children = new ArrayList<>();
    int currentLevel = context.getLevel();
    if (sameLevelMemNodes == null) {
      sameLevelMemNodes = new LinkedList<>();
      // process the current memory node
      levelProcess.handle(memNode, request, context);
      // get all memory nodes to be processed in the next layer
      children = levelProcess.getChildren(memNode, request, context);
    } else {
      while (!sameLevelMemNodes.isEmpty()) {
        I node = (I) sameLevelMemNodes.poll();
        levelProcess.handle(node, request, context);
        children.addAll(levelProcess.getChildren(node, request, context));
      }
    }
    sameLevelMemNodes.addAll(children);
    context.setLevel(currentLevel + 1);
    if (levelProcess.hasNext() && !sameLevelMemNodes.isEmpty()) {
      levelProcess.getNext().process(null, request, context);
    }
  }
}
