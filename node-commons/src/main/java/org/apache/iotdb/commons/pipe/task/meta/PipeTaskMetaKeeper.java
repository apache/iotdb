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
package org.apache.iotdb.commons.pipe.task.meta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class PipeTaskMetaKeeper {

  protected final Map<String, PipeTaskMeta> pipeNameToPipeTaskMetaMap;

  public PipeTaskMetaKeeper() {
    pipeNameToPipeTaskMetaMap = new ConcurrentHashMap<>();
  }

  public void addPipeTaskMeta(PipeTaskMeta pipeTaskMeta) {
    pipeNameToPipeTaskMetaMap.put(pipeTaskMeta.getPipeName().toUpperCase(), pipeTaskMeta);
  }

  public void removePipeTaskMeta(String TaskName) {
    pipeNameToPipeTaskMetaMap.remove(TaskName.toUpperCase());
  }

  public PipeTaskMeta getPipeTaskMeta(String TaskName) {
    return pipeNameToPipeTaskMetaMap.get(TaskName.toUpperCase());
  }

  public PipeTaskMeta[] getAllPipeTaskMeta() {
    return pipeNameToPipeTaskMetaMap.values().toArray(new PipeTaskMeta[0]);
  }

  public boolean containsPipeTask(String TaskName) {
    return pipeNameToPipeTaskMetaMap.containsKey(TaskName.toUpperCase());
  }
}
