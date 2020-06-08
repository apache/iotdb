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
package org.apache.iotdb.db.engine.memtable.nvm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.memtable.AbstractMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.NVMTVList;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVMPrimitiveMemTable extends AbstractMemTable {

  private static final Logger logger = LoggerFactory.getLogger(NVMPrimitiveMemTable.class);

  public NVMPrimitiveMemTable(String sgId) {
    super(sgId);
  }

  public NVMPrimitiveMemTable(Map<String, Map<String, IWritableMemChunk>> memTableMap, String sgId) {
    super(memTableMap, sgId);
  }

  @Override
  protected IWritableMemChunk genMemSeries(String deviceId, String measurementId, MeasurementSchema schema) {
    return new NVMWritableMemChunk(schema,
        (NVMTVList) TVListAllocator.getInstance().allocate(storageGroupId, deviceId, measurementId, schema.getType(), true));
  }

  @Override
  public IMemTable copy() {
    Map<String, Map<String, IWritableMemChunk>> newMap = new HashMap<>(getMemTableMap());

    return new NVMPrimitiveMemTable(newMap, storageGroupId);
  }

  @Override
  public boolean isSignalMemTable() {
    return false;
  }

  @Override
  public int hashCode() {return (int) getVersion();}

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  public void loadData(Map<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>> dataMap) {
    if (dataMap == null) {
      return;
    }

    for (Entry<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>> deviceDataEntry : dataMap
        .entrySet()) {
      String deviceId = deviceDataEntry.getKey();
      Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>> dataOfDevice = deviceDataEntry.getValue();
      for (Entry<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>> measurementDataEntry : dataOfDevice
          .entrySet()) {
        String measurementId = measurementDataEntry.getKey();
        Pair<List<NVMDataSpace>, List<NVMDataSpace>> tvListPair = measurementDataEntry.getValue();

        try {
          MeasurementSchema[] schemas = MManager.getInstance().getSchemas(deviceId, new String[]{measurementId});
          NVMWritableMemChunk memChunk = (NVMWritableMemChunk) createIfNotExistAndGet(deviceId, measurementId, schemas[0]);
          memChunk.loadData(tvListPair.left, tvListPair.right);
        } catch (MetadataException e) {
          logger.error(
              "occurs exception when reloading records from path ({}.{}): {}.(Will ignore the records)",
              deviceId, measurementId, e.getMessage());
        }
      }
    }
  }
}
