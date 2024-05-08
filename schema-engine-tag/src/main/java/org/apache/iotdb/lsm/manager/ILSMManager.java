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

import org.apache.iotdb.lsm.context.requestcontext.RequestContext;
import org.apache.iotdb.lsm.levelProcess.LevelProcessorChain;
import org.apache.iotdb.lsm.request.IRequest;

// Represents the manager of the lsm framework, used to handle root memory node
public interface ILSMManager<T, R extends IRequest, C extends RequestContext> {

  /**
   * preprocessing of the root memory node
   *
   * @param root root memory node
   * @param context request context
   */
  void preProcess(T root, R request, C context);

  /**
   * postprocessing of the root memory node
   *
   * @param root root memory node
   * @param context request context
   */
  void postProcess(T root, R request, C context);

  /**
   * use this method to process root memory node
   *
   * @param memNode memory node
   * @param context request context
   */
  void process(T memNode, R request, C context);

  /**
   * set level processors chain
   *
   * @param levelProcessorsChain level processors chain
   */
  void setLevelProcessorsChain(LevelProcessorChain<T, R, C> levelProcessorsChain);
}
