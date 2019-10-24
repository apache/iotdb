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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;

public class AddPathPlan extends PhysicalPlan {

  private Path path;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressor;
  private Map<String, String> props;
	  
  public AddPathPlan(Path path, TSDataType dataType, TSEncoding encoding, 
      CompressionType compressor, Map<String, String> props) {
    super(false, Operator.OperatorType.CREATE_TIMESERIES);
    this.path = path;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    this.props = props;
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
    String ret = String.format("seriesPath: %s%nresultDataType: %s%nencoding: %s%nnamespace type:"
        + " ADD_PATH%nargs: ", path, dataType, encoding);
    StringBuilder stringBuilder = new StringBuilder(ret.length()+50);
    stringBuilder.append(ret);
    for (Map.Entry<String, String> prop : props.entrySet()) {
      stringBuilder.append(prop.getKey()).append("=").append(prop.getValue()).append(",");
    }
    return stringBuilder.toString();
  }
  
  @Override
  public List<Path> getPaths() {
    List<Path> ret = new ArrayList<>();
    if (path != null) {
      ret.add(path);
    }
    return ret;
  }

}
