/**
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
package org.apache.iotdb.db.qp.logical.sys;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * this class maintains information in Metadata.namespace statement
 */
public class MetadataOperator extends RootOperator {

  private final NamespaceType namespaceType;
  private Path path;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressor;
  private Map<String, String> props;
  private List<Path> deletePathList;

  /**
   * Constructor of MetadataOperator.
   */
  public MetadataOperator(int tokenIntType, NamespaceType type) {
    super(tokenIntType);
    namespaceType = type;
    switch (type) {
      case DELETE_STORAGE_GROUP:
        operatorType = OperatorType.DELETE_STORAGE_GROUP;
        break;
      case SET_STORAGE_GROUP:
        operatorType = OperatorType.SET_STORAGE_GROUP;
        break;
      case ADD_PATH:
        operatorType = OperatorType.CREATE_TIMESERIES;
        break;
      case DELETE_PATH:
        operatorType = OperatorType.DELETE_TIMESERIES;
        break;
      default:
        break;
    }
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

  public TSEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }

  public void setCompressor(CompressionType compressor) {
    this.compressor = compressor;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public NamespaceType getNamespaceType() {
    return namespaceType;
  }

  public List<Path> getDeletePathList() {
    return deletePathList;
  }

  public void setDeletePathList(List<Path> deletePathList) {
    this.deletePathList = deletePathList;
  }

  public enum NamespaceType {
    ADD_PATH, DELETE_PATH, SET_STORAGE_GROUP, DELETE_STORAGE_GROUP;

    /**
     * deserialize short number.
     *
     * @param i short number
     * @return NamespaceType
     */
    public static NamespaceType deserialize(short i) {
      switch (i) {
        case 0:
          return ADD_PATH;
        case 1:
          return DELETE_PATH;
        case 2:
          return SET_STORAGE_GROUP;
        case 3:
          return DELETE_STORAGE_GROUP;
        default:
          return null;
      }
    }

    /**
     * serialize.
     *
     * @return short number
     */
    public short serialize() {
      switch (this) {
        case ADD_PATH:
          return 0;
        case DELETE_PATH:
          return 1;
        case SET_STORAGE_GROUP:
          return 2;
        case DELETE_STORAGE_GROUP:
          return 3;
        default:
          return -1;
      }
    }
  }
}
