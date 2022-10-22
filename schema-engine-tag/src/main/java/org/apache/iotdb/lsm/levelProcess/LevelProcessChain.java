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

public class LevelProcessChain<T, R, C extends RequestContext> {

  // the level process of the first layer of memory nodes
  LevelProcess<T, ?, R, C> headLevelProcess;

  public <O> LevelProcess<T, O, R, C> nextLevel(LevelProcess<T, O, R, C> next) {
    this.headLevelProcess = next;
    return next;
  }

  public void process(T memNode, R request, C context) {
    headLevelProcess.process(memNode, request, context);
  }
}
