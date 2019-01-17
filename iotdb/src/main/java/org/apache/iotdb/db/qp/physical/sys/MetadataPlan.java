/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

public class MetadataPlan extends PhysicalPlan {

  private final MetadataOperator.NamespaceType namespaceType;
  private Path path;
  private String dataType;
  private String encoding;
  private String[] encodingArgs;

  private List<Path> deletePathList;

  /**
   * Constructor of MetadataPlan.
   */
  public MetadataPlan(MetadataOperator.NamespaceType namespaceType, Path path, String dataType,
      String encoding,
      String[] encodingArgs, List<Path> deletePathList) {
    super(false, Operator.OperatorType.METADATA);
    this.namespaceType = namespaceType;
    this.path = path;
    this.dataType = dataType;
    this.encoding = encoding;
    this.encodingArgs = encodingArgs;
    this.deletePathList = deletePathList;
    switch (namespaceType) {
      case SET_FILE_LEVEL:
      case ADD_PATH:
        setOperatorType(Operator.OperatorType.SET_STORAGE_GROUP);
        break;
      case DELETE_PATH:
        setOperatorType(Operator.OperatorType.DELETE_TIMESERIES);
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

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public String[] getEncodingArgs() {
    return encodingArgs;
  }

  public void setEncodingArgs(String[] encodingArgs) {
    this.encodingArgs = encodingArgs;
  }

  public MetadataOperator.NamespaceType getNamespaceType() {
    return namespaceType;
  }

  @Override
  public String toString() {
    String ret = "seriesPath: " + path + "\ndataType: " + dataType + "\nencoding: " + encoding
        + "\nnamespace type: " + namespaceType + "\nargs: ";
    for (String arg : encodingArgs) {
      ret = ret + arg + ",";
    }
    return ret;
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
}
