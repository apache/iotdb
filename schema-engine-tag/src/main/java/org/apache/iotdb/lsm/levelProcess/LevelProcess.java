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
package org.apache.iotdb.lsm.levelProcess;

import org.apache.iotdb.lsm.context.RequestContext;

/** the processing method corresponding to each layer of memory nodes */
public interface LevelProcess<I, O, R, C extends RequestContext> {

  /**
   * add the LevelProcess of the next layer of memory nodes
   *
   * @param next LevelProcess of the next layer
   * @return LevelProcess of the next layer
   */
  <T> LevelProcess<O, T, R, C> nextLevel(LevelProcess<O, T, R, C> next);

  /**
   * use this method to process memory nodes at each layer according to the access strategy
   *
   * @param memNode memory node
   * @param context request context
   */
  void process(I memNode, R request, C context);
}
