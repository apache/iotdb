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

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeMeta {

  private final PipeStaticMeta staticMeta;
  private final PipeRuntimeMeta runtimeMeta;

  public PipeMeta(PipeStaticMeta staticMeta, PipeRuntimeMeta runtimeMeta) {
    this.staticMeta = staticMeta;
    this.runtimeMeta = runtimeMeta;
  }

  public PipeStaticMeta getStaticMeta() {
    return staticMeta;
  }

  public PipeRuntimeMeta getRuntimeMeta() {
    return runtimeMeta;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(staticMeta.serialize(), outputStream);
    ReadWriteIOUtils.write(runtimeMeta.serialize(), outputStream);
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(staticMeta.serialize(), outputStream);
    ReadWriteIOUtils.write(runtimeMeta.serialize(), outputStream);
  }

  public static PipeMeta deserialize(FileInputStream fileInputStream) throws IOException {
    PipeStaticMeta staticMeta = PipeStaticMeta.deserialize(fileInputStream);
    PipeRuntimeMeta runtimeMeta = PipeRuntimeMeta.deserialize(fileInputStream);
    return new PipeMeta(staticMeta, runtimeMeta);
  }

  public static PipeMeta deserialize(ByteBuffer byteBuffer) throws IOException {
    PipeStaticMeta staticMeta = PipeStaticMeta.deserialize(byteBuffer);
    PipeRuntimeMeta runtimeMeta = PipeRuntimeMeta.deserialize(byteBuffer);
    return new PipeMeta(staticMeta, runtimeMeta);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeMeta pipeMeta = (PipeMeta) o;
    return Objects.equals(staticMeta, pipeMeta.staticMeta)
        && Objects.equals(runtimeMeta, pipeMeta.runtimeMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(staticMeta, runtimeMeta);
  }

  @Override
  public String toString() {
    return "PipeMeta{" + "staticMeta=" + staticMeta + ", runtimeMeta=" + runtimeMeta + '}';
  }
}
