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

import org.apache.iotdb.lsm.context.requestcontext.RequestContext;

import java.util.List;

/** the processing method corresponding to each layer of memory nodes */
public abstract class BasicLevelProcessor<I, O, R, C extends RequestContext>
    implements ILevelProcessor<I, O, R, C> {

  // the next level process
  ILevelProcessor<O, ?, R, C> next;

  /**
   * process the current layer memory node
   *
   * @param memNode memory node
   * @param context request context
   */
  public abstract void handle(I memNode, R request, C context);

  /**
   * get the memory node that needs to be processed in the next layer
   *
   * @param memNode memory node
   * @param context request context
   * @return all next-level memory nodes that need to be processed
   */
  public abstract List<O> getChildren(I memNode, R request, C context);

  /**
   * add the LevelProcess of the next layer of memory nodes
   *
   * @param next LevelProcess of the next layer
   * @return the next level process
   */
  @Override
  public <T> ILevelProcessor<O, T, R, C> nextLevel(ILevelProcessor<O, T, R, C> next) {
    this.next = next;
    return next;
  }

  /**
   * use this method to process memory nodes at each layer according to the access strategy
   *
   * @param memNode memory node
   * @param context request context
   */
  @Override
  public void process(I memNode, R request, C context) {
    context.getAccessStrategy().execute(this, memNode, request, context);
  }

  public boolean hasNext() {
    return next != null;
  }

  public ILevelProcessor<O, ?, R, C> getNext() {
    return next;
  }
}
