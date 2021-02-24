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

package org.apache.iotdb.db.query.executor.fill;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import java.io.IOException;
import java.util.Set;

public abstract class IFill {

  protected long queryTime;
  protected TSDataType dataType;

  public IFill(TSDataType dataType, long queryTime) {
    this.dataType = dataType;
    this.queryTime = queryTime;
  }

  public IFill() {}

  public abstract IFill copy();

  public abstract void configureFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      Set<String> deviceMeasurements,
      QueryContext context);

  public abstract TimeValuePair getFillResult()
      throws IOException, QueryProcessException, StorageEngineException;

  public TSDataType getDataType() {
    return this.dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public void setQueryTime(long queryTime) {
    this.queryTime = queryTime;
  }

  public long getQueryTime() {
    return queryTime;
  }

  abstract void constructFilter();
}
