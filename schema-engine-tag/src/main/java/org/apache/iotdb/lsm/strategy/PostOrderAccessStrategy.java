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

import org.apache.iotdb.lsm.context.RequestContext;
import org.apache.iotdb.lsm.levelProcess.BasicLevelProcess;

import java.util.List;

/** post-order traversal access strategy implementation class */
public class PostOrderAccessStrategy implements AccessStrategy {

  /**
   * post-order traversal access strategy
   *
   * @param levelProcess current level process
   * @param memNode memory node
   * @param context request context
   */
  @Override
  public <I, O, C extends RequestContext> void execute(
      BasicLevelProcess<I, O, C> levelProcess, I memNode, C context) {
    int currentLevel = context.getLevel();
    AccessStrategy accessStrategy = context.getAccessStrategy();
    // get all memory nodes to be processed in the next layer
    List<O> children = levelProcess.getChildren(memNode, context);
    if (levelProcess.hasNext()) {
      context.setLevel(currentLevel + 1);
      for (O child : children) {
        // process next level memory node
        levelProcess.getNext().process(child, context);
      }
    }

    context.setLevel(currentLevel);
    context.setAccessStrategy(accessStrategy);
    // process the current memory node
    levelProcess.handle(memNode, context);
  }
}
