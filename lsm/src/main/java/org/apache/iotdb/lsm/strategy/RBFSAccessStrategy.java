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

import java.util.List;

public class RBFSAccessStrategy implements AccessStrategy {
  @Override
  public <I, O, C extends Context> void execute(
      BasicLevelProcess<I, O, C> levelProcess, I memNode, C context) {
    int currentLevel = context.getLevel();
    if (Integer.MAX_VALUE == context.getLevelUpperBound() && !levelProcess.hasNext()) {
      context.setLevelUpperBound(context.getLevel());
    }

    if (currentLevel == 0) {
      while (context.getLevelUpperBound() != currentLevel) {
        List<O> children = levelProcess.getChildren(memNode, context);
        for (O child : children) {

          context.setLevel(currentLevel + 1);
          levelProcess.getNext().process(child, context);
          context.setLevel(currentLevel);
        }

        context.setLevelUpperBound(context.getLevelUpperBound() - 1);
      }

      levelProcess.handle(memNode, context);
      return;
    }

    if (currentLevel > context.getLevelUpperBound()) return;

    if (currentLevel == context.getLevelUpperBound()) {
      levelProcess.handle(memNode, context);
      return;
    }
    List<O> children = levelProcess.getChildren(memNode, context);
    for (O child : children) {
      context.setLevel(currentLevel + 1);
      levelProcess.getNext().process(child, context);
      context.setLevel(currentLevel);
    }
  }
}
