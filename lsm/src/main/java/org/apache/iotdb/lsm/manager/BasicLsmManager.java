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

import org.apache.iotdb.lsm.context.RequestContext;
import org.apache.iotdb.lsm.levelProcess.LevelProcess;

/** */
public class BasicLsmManager<T, C extends RequestContext> implements LsmManager<T, C> {

  // the level process of the first layer of memory nodes
  LevelProcess<T, ?, C> levelProcess;

  /**
   * preprocessing of the root memory node
   *
   * @param root root memory node
   * @param context request context
   * @throws Exception
   */
  public void preProcess(T root, C context) throws Exception {}

  /**
   * postprocessing of the root memory node
   *
   * @param root root memory node
   * @param context request context
   * @throws Exception
   */
  public void postProcess(T root, C context) throws Exception {}

  /**
   * processing of the root memory node
   *
   * @param root root memory node
   * @param context request context
   * @throws Exception
   */
  @Override
  public void process(T root, C context) throws Exception {
    preProcess(root, context);
    levelProcess.process(root, context);
    postProcess(root, context);
  }

  /**
   * add the LevelProcess of the next layer of memory nodes
   *
   * @param levelProcess LevelProcess of the next layer
   * @return LevelProcess of the next layer
   */
  @Override
  public <O> LevelProcess<T, O, C> nextLevel(LevelProcess<T, O, C> levelProcess) {
    this.levelProcess = levelProcess;
    return levelProcess;
  }
}
