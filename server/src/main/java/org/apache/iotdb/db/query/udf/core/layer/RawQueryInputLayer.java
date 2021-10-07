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

package org.apache.iotdb.db.query.udf.core.layer;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithValueFilter;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.dataset.UDFInputDataSet;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.core.layer.SafetyLine.SafetyPile;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.List;

public class RawQueryInputLayer {

  private UDFInputDataSet queryDataSet;
  private TSDataType[] dataTypes;
  private int timestampIndex;

  private ElasticSerializableRowRecordList rowRecordList;
  private SafetyLine safetyLine;

  /** InputLayerWithoutValueFilter */
  public RawQueryInputLayer(
      long queryId,
      float memoryBudgetInMB,
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      List<ManagedSeriesReader> readers)
      throws QueryProcessException, IOException, InterruptedException {
    construct(
        queryId,
        memoryBudgetInMB,
        new RawQueryDataSetWithoutValueFilter(queryId, paths, dataTypes, readers, true));
  }

  /** InputLayerWithValueFilter */
  public RawQueryInputLayer(
      long queryId,
      float memoryBudgetInMB,
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      TimeGenerator timeGenerator,
      List<IReaderByTimestamp> readers,
      List<Boolean> cached)
      throws QueryProcessException {
    construct(
        queryId,
        memoryBudgetInMB,
        new RawQueryDataSetWithValueFilter(paths, dataTypes, timeGenerator, readers, cached, true));
  }

  private void construct(long queryId, float memoryBudgetInMB, UDFInputDataSet queryDataSet)
      throws QueryProcessException {
    this.queryDataSet = queryDataSet;
    dataTypes = queryDataSet.getDataTypes().toArray(new TSDataType[0]);
    timestampIndex = dataTypes.length;
    rowRecordList =
        new ElasticSerializableRowRecordList(
            dataTypes, queryId, memoryBudgetInMB, 1 + dataTypes.length / 2);
    safetyLine = new SafetyLine();
  }

  public void updateRowRecordListEvictionUpperBound() {
    rowRecordList.setEvictionUpperBound(safetyLine.getSafetyLine());
  }

  public LayerPointReader constructPointReader(int columnIndex) {
    return new InputLayerPointReader(columnIndex);
  }

  private class InputLayerPointReader implements LayerPointReader {

    private final SafetyPile safetyPile;

    private final int columnIndex;
    private int currentRowIndex;

    private boolean hasCachedRowRecord;
    private Object[] cachedRowRecord;

    InputLayerPointReader(int columnIndex) {
      safetyPile = safetyLine.addSafetyPile();

      this.columnIndex = columnIndex;
      currentRowIndex = -1;

      hasCachedRowRecord = false;
      cachedRowRecord = null;
    }

    @Override
    public boolean next() throws IOException, QueryProcessException {
      if (hasCachedRowRecord) {
        return true;
      }

      for (int i = currentRowIndex + 1; i < rowRecordList.size(); ++i) {
        Object[] rowRecordCandidate = rowRecordList.getRowRecord(i);
        if (rowRecordCandidate[columnIndex] != null) {
          hasCachedRowRecord = true;
          cachedRowRecord = rowRecordCandidate;
          currentRowIndex = i;
          break;
        }
      }

      if (!hasCachedRowRecord) {
        while (queryDataSet.hasNextRowInObjects()) {
          Object[] rowRecordCandidate = queryDataSet.nextRowInObjects();
          rowRecordList.put(rowRecordCandidate);
          if (rowRecordCandidate[columnIndex] != null) {
            hasCachedRowRecord = true;
            cachedRowRecord = rowRecordCandidate;
            currentRowIndex = rowRecordList.size() - 1;
            break;
          }
        }
      }

      return hasCachedRowRecord;
    }

    @Override
    public void readyForNext() {
      hasCachedRowRecord = false;
      cachedRowRecord = null;

      safetyPile.moveForwardTo(currentRowIndex + 1);
    }

    @Override
    public TSDataType getDataType() {
      return dataTypes[columnIndex];
    }

    @Override
    public long currentTime() {
      return (long) cachedRowRecord[timestampIndex];
    }

    @Override
    public int currentInt() {
      return (int) cachedRowRecord[columnIndex];
    }

    @Override
    public long currentLong() {
      return (long) cachedRowRecord[columnIndex];
    }

    @Override
    public float currentFloat() {
      return (float) cachedRowRecord[columnIndex];
    }

    @Override
    public double currentDouble() {
      return (double) cachedRowRecord[columnIndex];
    }

    @Override
    public boolean currentBoolean() {
      return (boolean) cachedRowRecord[columnIndex];
    }

    @Override
    public Binary currentBinary() {
      return (Binary) cachedRowRecord[columnIndex];
    }
  }
}
