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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeTemporaryMeta {
  private final ConcurrentMap<Integer, Integer> completedDataNode = new ConcurrentHashMap<>();

  public void putDataNodeCompletion(final int dataNodeId) {
    completedDataNode.put(dataNodeId, dataNodeId);
  }

  public Set<Integer> getCompletedDataNode() {
    return completedDataNode.keySet();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeTemporaryMeta pipeTemporaryMeta = (PipeTemporaryMeta) o;
    return Objects.equals(completedDataNode, pipeTemporaryMeta.completedDataNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(completedDataNode);
  }

  @Override
  public String toString() {
    return "PipeMeta{" + "completedDataNode=" + completedDataNode + '}';
  }
}
