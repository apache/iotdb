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

  ContextType type;

  AccessStrategy accessStrategy;

  int level;

  int threadNums;

  int levelUpperBound;

  Object result;

  boolean recover;

  public Context() {
    accessStrategy = new PreOrderAccessStrategy();
    type = ContextType.NONE;
    level = 0;
    threadNums = 1;
    levelUpperBound = Integer.MAX_VALUE;
    recover = false;
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

  public int getLevelUpperBound() {
    return levelUpperBound;
  }

  public void setLevelUpperBound(int levelUpperBound) {
    this.levelUpperBound = levelUpperBound;
  }

  public boolean isRecover() {
    return recover;
  }

  public void setRecover(boolean recover) {
    this.recover = recover;
  }
}
