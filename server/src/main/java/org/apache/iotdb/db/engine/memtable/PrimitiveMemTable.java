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

package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.HashMap;
import java.util.Map;

public class PrimitiveMemTable extends AbstractMemTable {

  public PrimitiveMemTable() {}

  public PrimitiveMemTable(boolean enableMemControl) {
    this.disableMemControl = !enableMemControl;
  }

  public PrimitiveMemTable(Map<String, Map<String, IWritableMemChunk>> memTableMap) {
    super(memTableMap);
  }

  @Override
  protected IWritableMemChunk genMemSeries(IMeasurementSchema schema) {
    if (schema.getType() == TSDataType.VECTOR) {
      return new WritableMemChunk(
          schema,
          TVListAllocator.getInstance().allocate(schema.getSubMeasurementsTSDataTypeList()));
    }
    return new WritableMemChunk(schema, TVListAllocator.getInstance().allocate(schema.getType()));
  }

  @Override
  public IMemTable copy() {
    Map<String, Map<String, IWritableMemChunk>> newMap = new HashMap<>(getMemTableMap());

    return new PrimitiveMemTable(newMap);
  }

  @Override
  public boolean isSignalMemTable() {
    return false;
  }

  @Override
  public String toString() {
    return "PrimitiveMemTable{planIndex=[" + getMinPlanIndex() + "," + getMaxPlanIndex() + "]}";
  }
}
