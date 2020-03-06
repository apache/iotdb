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
package org.apache.iotdb.db.qp.physical.sys;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;

public class CreateTimeSeriesPlan extends PhysicalPlan {

  private Path path;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressor;
  private Map<String, String> props;

  public CreateTimeSeriesPlan() {
    super(false, Operator.OperatorType.CREATE_TIMESERIES);
    canbeSplit = false;
  }

  public CreateTimeSeriesPlan(Path path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) {
    super(false, Operator.OperatorType.CREATE_TIMESERIES);
    this.path = path;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    this.props = props;
    canbeSplit = false;
  }
  
  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }
  
  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public void setCompressor(CompressionType compressor) {
    this.compressor = compressor;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }
  
  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }
  
  @Override
  public String toString() {
    return String.format("seriesPath: %s, resultDataType: %s, encoding: %s, compression: %s", path,
        dataType, encoding, compressor);
  }
  
  @Override
  public List<Path> getPaths() {
    return Collections.singletonList(path);
  }

  @Override
  public void serializeTo(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_TIMESERIES.ordinal());
    byte[] pathBytes = path.getFullPath().getBytes();
    stream.writeInt(pathBytes.length);
    stream.write(pathBytes);
    stream.write(dataType.ordinal());
    stream.write(encoding.ordinal());
    stream.write(compressor.ordinal());
  }

  @Override
  public void deserializeFrom(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] pathBytes = new byte[length];
    buffer.get(pathBytes);
    path = new Path(new String(pathBytes));
    dataType = TSDataType.values()[buffer.get()];
    encoding = TSEncoding.values()[buffer.get()];
    compressor = CompressionType.values()[buffer.get()];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateTimeSeriesPlan that = (CreateTimeSeriesPlan) o;
    return Objects.equals(path, that.path) &&
        dataType == that.dataType &&
        encoding == that.encoding &&
        compressor == that.compressor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, dataType, encoding, compressor);
  }
}
