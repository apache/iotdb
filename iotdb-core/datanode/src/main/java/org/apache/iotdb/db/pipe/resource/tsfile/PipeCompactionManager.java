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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.agent.task.subtask.connector.PipeConnectorSubtaskLifeCycle;
import org.apache.iotdb.db.pipe.agent.task.subtask.connector.PipeRealtimePriorityBlockingQueue;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.Event;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class PipeCompactionManager {

  private final Set<PipeConnectorSubtaskLifeCycle> pipeConnectorSubtaskLifeCycles =
      new CopyOnWriteArraySet<>();

  public void registerPipeConnectorSubtaskLifeCycle(
      final PipeConnectorSubtaskLifeCycle pipeConnectorSubtaskLifeCycle) {
    pipeConnectorSubtaskLifeCycles.add(pipeConnectorSubtaskLifeCycle);
  }

  public void deregisterPipeConnectorSubtaskLifeCycle(
      final PipeConnectorSubtaskLifeCycle pipeConnectorSubtaskLifeCycle) {
    pipeConnectorSubtaskLifeCycles.remove(pipeConnectorSubtaskLifeCycle);
  }

  public void emitResult(
      final String storageGroupName,
      final String dataRegionId,
      final long timePartition,
      final List<TsFileResource> seqFileResources,
      final List<TsFileResource> unseqFileResources,
      final List<TsFileResource> targetFileResources) {
    final Set<File> sourceFiles = new HashSet<>();
    seqFileResources.forEach(tsFileResource -> sourceFiles.add(tsFileResource.getTsFile()));
    unseqFileResources.forEach(tsFileResource -> sourceFiles.add(tsFileResource.getTsFile()));
    final Set<File> targetFiles =
        targetFileResources.stream().map(TsFileResource::getTsFile).collect(Collectors.toSet());

    for (final PipeConnectorSubtaskLifeCycle lifeCycle : pipeConnectorSubtaskLifeCycles) {
      final UnboundedBlockingPendingQueue<Event> pendingQueue = lifeCycle.getPendingQueue();
      // TODO: support non realtime priority blocking queue
      if (pendingQueue instanceof PipeRealtimePriorityBlockingQueue) {
        final PipeRealtimePriorityBlockingQueue realtimePriorityBlockingQueue =
            (PipeRealtimePriorityBlockingQueue) pendingQueue;
        realtimePriorityBlockingQueue.replace(dataRegionId, sourceFiles, targetFiles);
      }
    }
  }
}
