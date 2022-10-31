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
package org.apache.iotdb.lsm.context.requestcontext;

import org.apache.iotdb.lsm.response.IResponse;
import org.apache.iotdb.lsm.strategy.IAccessStrategy;
import org.apache.iotdb.lsm.strategy.PreOrderAccessStrategy;

/** represents the context of a request */
public class RequestContext {

  // memory Structure Access Policy
  IAccessStrategy accessStrategy;

  // the tree level of the currently pending memory node
  int level;

  // the maximum level of memory nodes that can be processed
  int levelUpperBound;

  // response, encapsulating the response value and exception information.
  IResponse response;

  public RequestContext() {
    // preorder traversal strategy is used by default
    accessStrategy = new PreOrderAccessStrategy();
    level = 0;
    levelUpperBound = Integer.MAX_VALUE;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public int getLevel() {
    return level;
  }

  public IAccessStrategy getAccessStrategy() {
    return accessStrategy;
  }

  public void setAccessStrategy(IAccessStrategy accessStrategy) {
    this.accessStrategy = accessStrategy;
  }

  public int getLevelUpperBound() {
    return levelUpperBound;
  }

  public void setLevelUpperBound(int levelUpperBound) {
    this.levelUpperBound = levelUpperBound;
  }

  public <R extends IResponse> R getResponse() {
    return (R) response;
  }

  public <R extends IResponse> void setResponse(R response) {
    this.response = response;
  }

  /**
   * get the result of the response
   *
   * @param <T> type of the result
   * @return response result
   */
  public <T> T getValue() {
    return (T) getResponse().getValue();
  }

  /**
   * set the result of the response
   *
   * @param value response result
   * @param <T> type of the response result
   */
  public <T> void setValue(T value) {
    response.setValue(value);
  }

  /**
   * If an exception needs to be thrown during the processing of the request, this method can be
   * used to accept the exception
   *
   * @param e Exception
   */
  public void thrownException(Exception e) {
    response.addException(e);
  }
}
