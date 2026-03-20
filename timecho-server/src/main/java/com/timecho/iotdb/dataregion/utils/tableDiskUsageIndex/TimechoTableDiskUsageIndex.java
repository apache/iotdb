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

package com.timecho.iotdb.dataregion.utils.tableDiskUsageIndex;

import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.TableDiskUsageIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.object.IObjectTableSizeIndexReader;

import com.timecho.iotdb.dataregion.utils.tableDiskUsageIndex.object.ObjectTableSizeIndexReader;
import com.timecho.iotdb.dataregion.utils.tableDiskUsageIndex.object.ObjectTableSizeIndexWriter;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TimechoTableDiskUsageIndex extends TableDiskUsageIndex {

  TimechoTableDiskUsageIndex() {}

  @Override
  public void writeObjectDelta(
      String database, int regionId, long timePartition, String table, long size, int num) {
    addOperationToQueue(
        new WriteObjectDeltaOperation(database, regionId, table, timePartition, size, num));
  }

  @Override
  public void registerRegion(DataRegion region) {
    RegisterRegionOperation operation = new RegisterRegionOperation(region);
    if (!addOperationToQueue(operation)) {
      return;
    }
    CompletableFuture<Void> recoverFuture = operation.getFuture();
    try {
      recoverFuture.get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected DataRegionTableSizeIndexWriter createWriter(
      String database, int regionId, DataRegion region) {
    return new TimechoDataRegionTableSizeIndexWriter(database, regionId, region);
  }

  @Override
  protected IObjectTableSizeIndexReader createObjectFileIndexReader(
      DataRegionTableSizeIndexWriter dataRegionWriter, int regionId) {
    ObjectTableSizeIndexWriter objectTableSizeIndexWriter =
        ((TimechoDataRegionTableSizeIndexWriter) dataRegionWriter).objectTableSizeIndexWriter;
    return new ObjectTableSizeIndexReader(
        objectTableSizeIndexWriter.getFile(), objectTableSizeIndexWriter.getFile().length());
  }

  @Override
  protected void persistPendingObjectDeltasIfNecessary(
      DataRegionTableSizeIndexWriter dataRegionTableSizeIndexWriter) {
    TimechoDataRegionTableSizeIndexWriter writer =
        (TimechoDataRegionTableSizeIndexWriter) dataRegionTableSizeIndexWriter;
    writer.objectTableSizeIndexWriter.syncPendingObjectDeltasToDiskIfNecessary();
  }

  private static class WriteObjectDeltaOperation extends Operation {

    private final String table;
    private final long timePartition;
    private final long size;
    private final int num;

    protected WriteObjectDeltaOperation(
        String database, int regionId, String table, long timePartition, long size, int num) {
      super(database, regionId);
      this.table = table;
      this.timePartition = timePartition;
      this.size = size;
      this.num = num;
    }

    @Override
    public void apply(TableDiskUsageIndex tableDiskUsageIndex) {
      TimechoTableDiskUsageIndex timechoTableDiskUsageIndex =
          (TimechoTableDiskUsageIndex) tableDiskUsageIndex;
      TimechoDataRegionTableSizeIndexWriter writer =
          (TimechoDataRegionTableSizeIndexWriter)
              timechoTableDiskUsageIndex.writerMap.get(regionId);
      if (writer == null) {
        return;
      }
      writer.objectTableSizeIndexWriter.write(table, timePartition, size, num);
    }
  }

  protected static class TimechoDataRegionTableSizeIndexWriter
      extends DataRegionTableSizeIndexWriter {
    private final ObjectTableSizeIndexWriter objectTableSizeIndexWriter;

    protected TimechoDataRegionTableSizeIndexWriter(
        String database, int regionId, DataRegion dataRegion) {
      super(database, regionId, dataRegion);
      objectTableSizeIndexWriter = new ObjectTableSizeIndexWriter(database, regionId);
    }

    @Override
    public void compactIfNecessary() {
      super.compactIfNecessary();
      if (objectTableSizeIndexWriter.needCompact()) {
        objectTableSizeIndexWriter.compact();
      }
    }

    @Override
    public void closeIfIdle() {
      super.closeIfIdle();
      objectTableSizeIndexWriter.closeIfIdle();
    }

    @Override
    public void flush() throws IOException {
      super.flush();
      objectTableSizeIndexWriter.persistPendingObjectDeltas();
      objectTableSizeIndexWriter.flush();
    }

    @Override
    public void close() {
      super.close();
      objectTableSizeIndexWriter.close();
    }
  }
}
