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
package org.apache.iotdb.session.template;

import org.apache.iotdb.isession.template.TemplateNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;

public class MeasurementNode extends TemplateNode {
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressionType;

  public MeasurementNode(
      String name, TSDataType dataType, TSEncoding encoding, CompressionType compressionType) {
    super(name);
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressionType = compressionType;
  }

  @Override
  public boolean isMeasurement() {
    return true;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  /** Serialization: nodeName[String] dataType[byte] encoding[byte] compressor[byte] */
  @Override
  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.getName(), stream);
    ReadWriteIOUtils.write(getDataType().serialize(), stream);
    ReadWriteIOUtils.write(getEncoding().serialize(), stream);
    ReadWriteIOUtils.write(getCompressionType().serialize(), stream);
  }
}
