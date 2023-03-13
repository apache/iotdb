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

package org.apache.iotdb.commons.pipe.plugin.meta;

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipePluginMeta {
  private String pluginName;
  private String className;
  private String pluginType;
  private String jarName;
  private String jarMD5;

  private PipePluginMeta() {}

  public PipePluginMeta(String pluginName, String className, String pluginType) {
    this.pluginName = pluginName;
    this.className = className;
    this.pluginType = pluginType;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getClassName() {
    return className;
  }

  public String getPluginType() {
    return pluginType;
  }

  public String getJarName() {
    return jarName;
  }

  public String getJarMD5() {
    return jarMD5;
  }

  public void setPluginName(String pluginName) {
    this.pluginName = pluginName.toUpperCase();
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public void setPluginType(String pluginType) {
    this.pluginType = pluginType;
  }

  public void setJarName(String jarName) {
    this.jarName = jarName;
  }

  public void setJarMD5(String jarMD5) {
    this.jarMD5 = jarMD5;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pluginName, outputStream);
    ReadWriteIOUtils.write(className, outputStream);
    ReadWriteIOUtils.write(pluginType, outputStream);
    ReadWriteIOUtils.write(jarName, outputStream);
    ReadWriteIOUtils.write(jarMD5, outputStream);
  }

  public static PipePluginMeta deserialize(ByteBuffer buffer) {
    PipePluginMeta pipePluginMeta = new PipePluginMeta();
    pipePluginMeta.setPluginName(Objects.requireNonNull(ReadWriteIOUtils.readString(buffer)));
    pipePluginMeta.setClassName(ReadWriteIOUtils.readString(buffer));
    pipePluginMeta.setPluginType(ReadWriteIOUtils.readString(buffer));
    pipePluginMeta.setJarName(ReadWriteIOUtils.readString(buffer));
    pipePluginMeta.setJarMD5(ReadWriteIOUtils.readString(buffer));
    return pipePluginMeta;
  }

  public static PipePluginMeta deserialize(InputStream inputStream) throws IOException {
    return deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream)));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipePluginMeta that = (PipePluginMeta) o;
    return Objects.equals(pluginName, that.pluginName)
        && Objects.equals(className, that.className)
        && Objects.equals(pluginType, that.pluginType)
        && Objects.equals(jarName, that.jarName)
        && Objects.equals(jarMD5, that.jarMD5);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pluginName);
  }
}
