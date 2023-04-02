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
package org.apache.iotdb.commons.pipe.meta;

import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMetaKeeper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class PipeMetaKeeper {

  protected final Map<String, PipeMeta> pipeNameToPipeMetaMap;

  PipeTaskMetaKeeper pipeTaskMetaKeeper;
  PipePluginMetaKeeper pipePluginMetaKeeper;

  public PipeMetaKeeper() {
    pipeNameToPipeMetaMap = new ConcurrentHashMap<>();
  }

  public void addPipeMeta(PipeMeta pipeMeta) {
    pipeNameToPipeMetaMap.put(pipeMeta.getPipeName().toUpperCase(), pipeMeta);
    pipeTaskMetaKeeper.getPipeTaskMetas().addAll(pipeMeta.getPipeTasks().values());
  }

  public void removePipeMeta(String pipeName) {
    pipeTaskMetaKeeper
        .getPipeTaskMetas()
        .removeAll(pipeNameToPipeMetaMap.get(pipeName).getPipeTasks().values());
    pipeNameToPipeMetaMap.remove(pipeName.toUpperCase());
  }

  public PipeMeta getPipeMeta(String pipeName) {
    return pipeNameToPipeMetaMap.get(pipeName.toUpperCase());
  }

  public PipeMeta[] getAllPipeMeta() {
    return pipeNameToPipeMetaMap.values().toArray(new PipeMeta[0]);
  }

  public boolean containsPipe(String pipeName) {
    return pipeNameToPipeMetaMap.containsKey(pipeName.toUpperCase());
  }

  public PipePluginMetaKeeper getPipePluginMetaKeeper() {
    return pipePluginMetaKeeper;
  }

  public PipeTaskMetaKeeper getPipeTaskMetaKeeper() {
    return pipeTaskMetaKeeper;
  }

  public void clear() {
    pipeNameToPipeMetaMap.clear();
    pipeTaskMetaKeeper.clear();
    pipePluginMetaKeeper.clear();
  }
}
