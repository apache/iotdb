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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.apache.tsfile.utils.PublicBAOS;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeMeta {

  private final PipeStaticMeta staticMeta;
  private final PipeRuntimeMeta runtimeMeta;

  // This is temporary information of pipe and will not be serialized.
  private final PipeTemporaryMeta temporaryMeta;

  public PipeMeta(final PipeStaticMeta staticMeta, final PipeRuntimeMeta runtimeMeta) {
    this(staticMeta, runtimeMeta, new PipeTemporaryMetaInCoordinator());
  }

  public PipeMeta(
      final PipeStaticMeta staticMeta,
      final PipeRuntimeMeta runtimeMeta,
      final PipeTemporaryMeta temporaryMeta) {
    this.staticMeta = staticMeta;
    this.runtimeMeta = runtimeMeta;
    this.temporaryMeta = temporaryMeta;
  }

  public PipeStaticMeta getStaticMeta() {
    return staticMeta;
  }

  public PipeRuntimeMeta getRuntimeMeta() {
    return runtimeMeta;
  }

  public PipeTemporaryMeta getTemporaryMeta() {
    return temporaryMeta;
  }

  public ByteBuffer serialize() throws IOException {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(final OutputStream outputStream) throws IOException {
    staticMeta.serialize(outputStream);
    runtimeMeta.serialize(outputStream);
  }

  public static PipeMeta deserialize(final FileInputStream fileInputStream) throws IOException {
    final PipeStaticMeta staticMeta = PipeStaticMeta.deserialize(fileInputStream);
    final PipeRuntimeMeta runtimeMeta = PipeRuntimeMeta.deserialize(fileInputStream);
    return new PipeMeta(staticMeta, runtimeMeta);
  }

  public static PipeMeta deserialize4TaskAgent(final ByteBuffer byteBuffer) {
    final PipeStaticMeta staticMeta = PipeStaticMeta.deserialize(byteBuffer);
    final PipeRuntimeMeta runtimeMeta = PipeRuntimeMeta.deserialize(byteBuffer);
    return new PipeMeta(
        staticMeta,
        runtimeMeta,
        new PipeTemporaryMetaInAgent(staticMeta.getPipeName(), staticMeta.getCreationTime()));
  }

  public static PipeMeta deserialize4Coordinator(final ByteBuffer byteBuffer) {
    final PipeStaticMeta staticMeta = PipeStaticMeta.deserialize(byteBuffer);
    final PipeRuntimeMeta runtimeMeta = PipeRuntimeMeta.deserialize(byteBuffer);
    return new PipeMeta(staticMeta, runtimeMeta);
  }

  public PipeMeta deepCopy4TaskAgent() throws IOException {
    return PipeMeta.deserialize4TaskAgent(serialize());
  }

  public String coreReportMessage() {
    return "PipeName="
        + staticMeta.getPipeName()
        + ", CreationTime="
        + staticMeta.getCreationTime()
        + ", ProgressIndex={"
        + runtimeMeta.getConsensusGroupId2TaskMetaMap().entrySet().stream()
            .map(
                entry ->
                    "ConsensusGroupId="
                        + entry.getKey()
                        + ", ProgressIndex="
                        + entry.getValue().getProgressIndex())
            .reduce((s1, s2) -> s1 + "; " + s2)
            .orElse("")
        + "}, Exceptions={"
        + runtimeMeta.getConsensusGroupId2TaskMetaMap().entrySet().stream()
            .filter(entry -> entry.getValue().hasExceptionMessages())
            .map(
                entry ->
                    "ConsensusGroupId="
                        + entry.getKey()
                        + ", ExceptionMessage="
                        + entry.getValue().getExceptionMessagesString())
            .reduce((s1, s2) -> s1 + "; " + s2)
            .orElse("")
        + "}";
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeMeta pipeMeta = (PipeMeta) o;
    return Objects.equals(staticMeta, pipeMeta.staticMeta)
        && Objects.equals(runtimeMeta, pipeMeta.runtimeMeta)
        && Objects.equals(temporaryMeta, pipeMeta.temporaryMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(staticMeta, runtimeMeta, temporaryMeta);
  }

  @Override
  public String toString() {
    return "PipeMeta{"
        + "staticMeta="
        + staticMeta
        + ", runtimeMeta="
        + runtimeMeta
        + ", temporaryMeta="
        + temporaryMeta
        + '}';
  }
}
