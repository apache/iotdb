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
import java.util.Objects;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;

public class MetadataPlan extends PhysicalPlan {

  private final MetadataOperator.NamespaceType namespaceType;
  private Path path;
  private TSDataType dataType;
  private CompressionType compressor;
  private TSEncoding encoding;
  private Map<String, String> props;

  private List<Path> deletePathList;

  /**
   * Constructor of MetadataPlan.
   */
  public MetadataPlan(MetadataOperator.NamespaceType namespaceType, Path path, TSDataType dataType,
      CompressionType compressor, TSEncoding encoding, Map<String, String> props,
      List<Path> deletePathList) {
    super(false, Operator.OperatorType.METADATA);
    this.namespaceType = namespaceType;
    this.path = path;
    this.dataType = dataType;
    this.compressor = compressor;
    this.encoding = encoding;
    this.props = props;
    this.deletePathList = deletePathList;
    setOperatorType(namespaceType);
  }

  public MetadataPlan(MetadataOperator.NamespaceType namespaceType, Path path, TSDataType dataType,
                      TSEncoding encoding, CompressionType compressor) {
    super(false, Operator.OperatorType.METADATA);
    this.namespaceType = namespaceType;
    this.path = path;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    setOperatorType(namespaceType);
  }

  public MetadataPlan(MetadataOperator.NamespaceType namespaceType, Path path) {
    super(false, Operator.OperatorType.METADATA);
    this.namespaceType = namespaceType;
    this.path = path;
    setOperatorType(namespaceType);
  }

  /**
   * delete time series plan
   * @param namespaceType
   * @param path
   */
  public MetadataPlan(MetadataOperator.NamespaceType namespaceType, List<Path> path) {
    super(false, Operator.OperatorType.METADATA);
    this.namespaceType = namespaceType;
    this.deletePathList = path;
    setOperatorType(namespaceType);
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

  public MetadataOperator.NamespaceType getNamespaceType() {
    return namespaceType;
  }

  @Override
  public String toString() {
    String ret = String.format("seriesPath: %s%nresultDataType: %s%nencoding: %s%nnamespace type:"
        + " %s%nargs: ", path, dataType, encoding, namespaceType);
    StringBuilder stringBuilder = new StringBuilder(ret.length()+50);
    stringBuilder.append(ret);
    for (Map.Entry prop : props.entrySet()) {
      stringBuilder.append(prop.getKey()).append("=").append(prop.getValue()).append(",");
    }
    return stringBuilder.toString();
  }

  public void addDeletePath(Path path) {
    deletePathList.add(path);
  }

  public List<Path> getDeletePathList() {
    return deletePathList;
  }

  @Override
  public List<Path> getPaths() {
    if (deletePathList != null) {
      return deletePathList;
    }

    List<Path> ret = new ArrayList<>();
    if (path != null) {
      ret.add(path);
    }
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetadataPlan)) {
      return false;
    }
    MetadataPlan that = (MetadataPlan) o;
    return getNamespaceType() == that.getNamespaceType() &&
        Objects.equals(getPath(), that.getPath()) &&
        getDataType() == that.getDataType() &&
        getCompressor() == that.getCompressor() &&
        getEncoding() == that.getEncoding() &&
        Objects.equals(getProps(), that.getProps()) &&
        Objects.equals(getDeletePathList(), that.getDeletePathList());
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(getNamespaceType(), getPath(), getDataType(), getCompressor(), getEncoding(),
            getProps(), getDeletePathList());
  }

  private void setOperatorType(MetadataOperator.NamespaceType namespaceType) {
    switch (namespaceType) {
      case SET_STORAGE_GROUP:
        setOperatorType(Operator.OperatorType.SET_STORAGE_GROUP);
        break;
      case ADD_PATH:
        setOperatorType(Operator.OperatorType.CREATE_TIMESERIES);
        break;
      case DELETE_PATH:
        setOperatorType(Operator.OperatorType.DELETE_TIMESERIES);
        break;
      default:
        break;
    }
  }
}
