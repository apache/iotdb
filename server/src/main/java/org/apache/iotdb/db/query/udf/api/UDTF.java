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

package org.apache.iotdb.db.query.udf.api;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.query.udf.api.collector.DataPointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.iterator.Iterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public abstract class UDTF implements UDF {

  protected int rowOffset;
  protected int rowLimit;

  protected List<Path> paths;
  protected List<TSDataType> dataTypes;

  protected List<Iterator> columnDataGenerators;
  protected Map<String, Iterator> rowDataGenerators;

  protected DataPointCollector collector;

  public abstract void initializeUDF(UDFParameters parameters, UDTFConfigurations configurations);

  public abstract boolean hasRemainingDataToTransform();

  public abstract void transform(int fetchSize);

  public final void setRowOffset(int rowOffset) {
    this.rowOffset = rowOffset;
  }

  public final void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  public final void setPaths(List<Path> paths) {
    this.paths = paths;
  }

  public final void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public final void setColumnDataGenerators(List<Iterator> columnDataGenerators) {
    this.columnDataGenerators = columnDataGenerators;
  }

  public final void setRowDataGenerators(Map<String, Iterator> rowDataGenerators) {
    this.rowDataGenerators = rowDataGenerators;
  }

  public final void setCollector(DataPointCollector collector) {
    this.collector = collector;
  }

  public final DataPointCollector getCollector() {
    return collector;
  }
}
