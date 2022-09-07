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
package org.apache.iotdb.lsm.manager;

import org.apache.iotdb.lsm.context.Context;
import org.apache.iotdb.lsm.levelProcess.LevelProcess;

public class BasicLsmManager<T, C extends Context> implements LsmManager<T, C> {

  T root;

  LevelProcess<T, ?, C> levelProcess;

  @Override
  public BasicLsmManager<T, C> manager(T memNode) {
    root = memNode;
    return this;
  }

  @Override
  public void process(C context) {
    levelProcess.process(root, context);
  }

  @Override
  public <O> LevelProcess<T, O, C> nextLevel(LevelProcess<T, O, C> levelProcess) {
    this.levelProcess = levelProcess;
    return levelProcess;
  }
}
