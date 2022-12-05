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

import java.util.List;

/** reverse breadth first traversal access strategy implementation class */
public class RBFSAccessStrategy implements IAccessStrategy {

  /**
   * reverse breadth first traversal access strategy
   *
   * @param levelProcess current level process
   * @param memNode memory node
   * @param context request context
   */
  @Override
  public <I, O, R, C extends RequestContext> void execute(
      BasicLevelProcessor<I, O, R, C> levelProcess, I memNode, R request, C context) {
    int currentLevel = context.getLevel();

    // if the upper bound has not been set and there is no next-level processing method, set the
    // upper bound to the current level
    if (Integer.MAX_VALUE == context.getLevelUpperBound() && !levelProcess.hasNext()) {
      context.setLevelUpperBound(context.getLevel());
    }

    // if the current memory node is the root
    if (currentLevel == 0) {
      // if all the next level nodes of the root node have not been processed
      while (context.getLevelUpperBound() != currentLevel) {
        // process all pending next-level nodes
        List<O> children = levelProcess.getChildren(memNode, request, context);
        for (O child : children) {
          context.setLevel(currentLevel + 1);
          // use the processing method of the next layer to process the next layer of nodes
          levelProcess.getNext().process(child, request, context);
          context.setLevel(currentLevel);
        }

        // after each layer is processed, the upper bound is reduced by one
        context.setLevelUpperBound(context.getLevelUpperBound() - 1);
      }

      // process the current memory node
      levelProcess.handle(memNode, request, context);
      return;
    }

    if (currentLevel > context.getLevelUpperBound()) return;

    // only process memory nodes with equal level and upper bound
    if (currentLevel == context.getLevelUpperBound()) {
      levelProcess.handle(memNode, request, context);
      return;
    }

    // process all pending next-level nodes
    List<O> children = levelProcess.getChildren(memNode, request, context);
    for (O child : children) {
      context.setLevel(currentLevel + 1);
      levelProcess.getNext().process(child, request, context);
      context.setLevel(currentLevel);
    }
  }
}
