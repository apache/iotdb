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
package org.apache.iotdb.lsm.context;

import org.apache.iotdb.lsm.strategy.AccessStrategy;
import org.apache.iotdb.lsm.strategy.PreOrderAccessStrategy;

public class Context {

  // 类型
  ContextType type;

  // 访问策略
  AccessStrategy accessStrategy;

  // 所处的树深度
  int level;

  // 多少个线程处理该节点的子节点
  int threadNums;

  // 返回值
  Object result;

  public Context() {
    accessStrategy = new PreOrderAccessStrategy();
    type = ContextType.NONE;
    level = 0;
    threadNums = 1;
  }

  public Context(
      ContextType type,
      AccessStrategy accessStrategy,
      int level,
      boolean sync,
      int threadNums,
      Object result) {
    this.type = type;
    this.accessStrategy = accessStrategy;
    this.level = level;
    this.threadNums = threadNums;
    this.result = result;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public int getLevel() {
    return level;
  }

  public ContextType getType() {
    return type;
  }

  public int getThreadNums() {
    return threadNums;
  }

  public void setType(ContextType type) {
    this.type = type;
  }

  public void setThreadNums(int threadNums) {
    this.threadNums = threadNums;
  }

  public Object getResult() {
    return result;
  }

  public void setResult(Object result) {
    this.result = result;
  }

  public AccessStrategy getAccessStrategy() {
    return accessStrategy;
  }

  public void setAccessStrategy(AccessStrategy accessStrategy) {
    this.accessStrategy = accessStrategy;
  }
}
