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

package org.apache.iotdb.confignode.manager.pipe.event;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.manager.pipe.resource.snapshot.PipeConfigNodeSnapshotResourceManager;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;

public class PipeConfigRegionSnapshotEvent extends PipeSnapshotEvent {

  public PipeConfigRegionSnapshotEvent() {
    // Used for deserialization
    this(null);
  }

  public PipeConfigRegionSnapshotEvent(String snapshotPath) {
    this(snapshotPath, null, null, null);
  }

  public PipeConfigRegionSnapshotEvent(
      String snapshotPath, String pipeName, PipeTaskMeta pipeTaskMeta, String pattern) {
    super(
        snapshotPath,
        pipeName,
        pipeTaskMeta,
        pattern,
        PipeConfigNodeSnapshotResourceManager.getInstance());
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName, PipeTaskMeta pipeTaskMeta, String pattern, long startTime, long endTime) {
    return new PipeConfigRegionSnapshotEvent(snapshotPath, pipeName, pipeTaskMeta, pattern);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    ByteBuffer result =
        ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + snapshotPath.getBytes().length);
    ReadWriteIOUtils.write(PipeConfigSerializableEventType.CONFIG_SNAPSHOT.getType(), result);
    ReadWriteIOUtils.write(snapshotPath, result);
    return result;
  }
}
