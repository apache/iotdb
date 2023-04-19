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

package org.apache.iotdb.db.pipe.task;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeSubtaskManager {

  private final ConcurrentHashMap<String, AtomicInteger> alivePipePluginReferenceCountMap;
  private final ConcurrentHashMap<String, AtomicInteger> runtimePipePluginReferenceCountMap;

  public PipeSubtaskManager() {
    runtimePipePluginReferenceCountMap = new ConcurrentHashMap<>();
    alivePipePluginReferenceCountMap = new ConcurrentHashMap<>();
  }

  public int increaseAlivePipePluginRef(String pipePluginID) {
    return alivePipePluginReferenceCountMap
        .computeIfAbsent(pipePluginID, k -> new AtomicInteger(0))
        .incrementAndGet();
  }

  public boolean decreaseAlivePipePluginRef(String pipePluginID) {
    if (alivePipePluginReferenceCountMap.computeIfPresent(
            pipePluginID, (k, v) -> v.decrementAndGet() == 0 ? null : v)
        == null) {
      alivePipePluginReferenceCountMap.remove(pipePluginID);
    }
    return alivePipePluginReferenceCountMap.containsKey(pipePluginID);
  }

  public int increaseRuntimePipePluginRef(String pipePluginID) {
    return runtimePipePluginReferenceCountMap
        .computeIfAbsent(pipePluginID, k -> new AtomicInteger(0))
        .incrementAndGet();
  }

  public boolean decreaseRuntimePipePluginRef(String pipePluginID) {
    if (runtimePipePluginReferenceCountMap.computeIfPresent(
            pipePluginID, (k, v) -> v.decrementAndGet() == 0 ? null : v)
        == null) {
      runtimePipePluginReferenceCountMap.remove(pipePluginID);
    }
    return runtimePipePluginReferenceCountMap.containsKey(pipePluginID);
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private static class PipeSubtaskManagerHolder {
    private static PipeSubtaskManager instance = null;
  }

  public static PipeSubtaskManager setupAndGetInstance() {
    if (PipeSubtaskManagerHolder.instance == null) {
      PipeSubtaskManagerHolder.instance = new PipeSubtaskManager();
    }
    return PipeSubtaskManagerHolder.instance;
  }
}
