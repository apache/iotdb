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
 *
 */
package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginRegister;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

// TODO(Ext-pipe): move to nodes-common
public class ExternalPipeSink implements PipeSink {
  private static final Logger logger = LoggerFactory.getLogger(ExternalPipeSink.class);

  private static final PipeSinkType pipeSinkType = PipeSinkType.ExternalPipe;

  private String pipeSinkName;
  private String extPipeSinkTypeName;

  private Map<String, String> sinkParams;

  public ExternalPipeSink() {}

  public ExternalPipeSink(String pipeSinkName, String extPipeSinkTypeName) {
    this.pipeSinkName = pipeSinkName;
    this.extPipeSinkTypeName = extPipeSinkTypeName;
  }

  @Override
  public void setAttribute(Map<String, String> params) throws PipeSinkException {
    String regex = "^'|'$|^\"|\"$";
    sinkParams =
        params.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().trim().replaceAll(regex, ""),
                    (key1, key2) -> key2));

    try {
      ExtPipePluginRegister.getInstance()
          .getWriteFactory(extPipeSinkTypeName)
          .validateSinkParams(sinkParams);
    } catch (Exception e) {
      throw new PipeSinkException(e.getMessage());
    }
  }

  @Override
  public String getPipeSinkName() {
    return pipeSinkName;
  }

  @Override
  public PipeSinkType getType() {
    return pipeSinkType;
  }

  @Override
  public String showAllAttributes() {
    // HACK: should provide an interface 'getDisplayableAttributes(Map<String, String>)' and let the
    // plugin decide which attribute is suitable for being displayed.
    return sinkParams.entrySet().stream()
        .filter(e -> !e.getKey().contains("access_key"))
        .collect(Collectors.toList())
        .toString();
  }

  @Override
  public TPipeSinkInfo getTPipeSinkInfo() {
    return new TPipeSinkInfo(this.pipeSinkName, this.pipeSinkType.name()).setAttributes(sinkParams);
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write((byte) pipeSinkType.ordinal(), outputStream);
    ReadWriteIOUtils.write(pipeSinkName, outputStream);
    ReadWriteIOUtils.write(extPipeSinkTypeName, outputStream);
    ReadWriteIOUtils.write(sinkParams, outputStream);
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    pipeSinkName = ReadWriteIOUtils.readString(inputStream);
    extPipeSinkTypeName = ReadWriteIOUtils.readString(inputStream);
    sinkParams = ReadWriteIOUtils.readMap(inputStream);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    pipeSinkName = ReadWriteIOUtils.readString(buffer);
    extPipeSinkTypeName = ReadWriteIOUtils.readString(buffer);
    sinkParams = ReadWriteIOUtils.readMap(buffer);
  }

  public Map<String, String> getSinkParams() {
    return sinkParams;
  }

  public String getExtPipeSinkTypeName() {
    return extPipeSinkTypeName;
  }
}
