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

package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorTimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * An iterator of linked hashmaps ( measurement -> chunk metadata list ). When traversing the linked
 * hashmap, you will get chunk metadata lists according to the lexicographic order of the
 * measurements. The first measurement of the linked hashmap of each iteration is always larger than
 * the last measurement of the linked hashmap of the previous iteration in lexicographic order.
 */
public class MeasurementChunkMetadataListMapIterator
    implements Iterator<LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>> {

  private static final int MEASUREMENT_LIMIT =
      TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode();

  private final TsFileSequenceReader reader;
  private final String device;

  private final Queue<TimeseriesMetadata> timeseriesMetadataQueue;
  private final Queue<Pair<Long, Long>> timeSeriesBufferOffsetRangeQueue;

  private final Map<String, TimeseriesMetadata> timeseriesMetadataCache;

  public MeasurementChunkMetadataListMapIterator(TsFileSequenceReader reader, String device)
      throws IOException {
    this.reader = reader;
    this.device = device;

    timeseriesMetadataQueue = new LinkedList<>();
    timeSeriesBufferOffsetRangeQueue = reader.getTimeSeriesBufferOffsetRangeQueue(device);

    timeseriesMetadataCache = new HashMap<>();

    collectAllTimeseriesMetadata();
  }

  private void collectAllTimeseriesMetadata() {
    for (; ; ) {
      if (!reader.collectTimeSeriesMetadata(
          timeSeriesBufferOffsetRangeQueue, timeseriesMetadataQueue, timeseriesMetadataCache)) {
        break;
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !timeseriesMetadataQueue.isEmpty() && !timeSeriesBufferOffsetRangeQueue.isEmpty();
  }

  @Override
  public LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>> measurementChunkMetadataList =
        new LinkedHashMap<>();

    int measurementCount = 0;
    while (measurementCount < MEASUREMENT_LIMIT) {
      if (timeseriesMetadataQueue.isEmpty()
          && !reader.collectTimeSeriesMetadata(
              timeSeriesBufferOffsetRangeQueue, timeseriesMetadataQueue, timeseriesMetadataCache)) {
        break;
      }

      TimeseriesMetadata timeseriesMetadata = timeseriesMetadataQueue.remove();
      try {
        switch (timeseriesMetadata.getTimeSeriesType()) {
          case COMMON:
            measurementCount +=
                handleCommonTimeSeries(timeseriesMetadata, measurementChunkMetadataList);
            break;
          case VECTOR_SERIES_TIME_COLUMN:
            measurementCount +=
                handleVectorSeriesTimeColumn(timeseriesMetadata, measurementChunkMetadataList);
            break;
          case VECTOR_SERIES_VALUE_COLUMN:
            break;
          default:
            throw new TsFileRuntimeException("Unsupported time series type.");
        }
      } catch (MetadataException | IOException e) {
        throw new TsFileRuntimeException(e.getMessage());
      }
    }

    return measurementChunkMetadataList;
  }

  private int handleCommonTimeSeries(
      TimeseriesMetadata timeseriesMetadata,
      LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>> measurementChunkMetadataList)
      throws MetadataException, IOException {
    List<IChunkMetadata> chunkMetadataList =
        measurementChunkMetadataList.computeIfAbsent(
            IoTDB.metaManager.getSeriesSchema(
                new PartialPath(device, timeseriesMetadata.getMeasurementId())),
            m -> new ArrayList<>());
    chunkMetadataList.addAll(timeseriesMetadata.loadChunkMetadataList());

    return 1;
  }

  private int handleVectorSeriesTimeColumn(
      TimeseriesMetadata timeseriesMetadata,
      LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>> measurementChunkMetadataList)
      throws MetadataException, IOException {
    List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();
    VectorMeasurementSchema vectorMeasurementSchema =
        (VectorMeasurementSchema)
            IoTDB.metaManager.getSeriesSchema(
                new PartialPath(device, timeseriesMetadata.getMeasurementId()));
    String[] measurements = vectorMeasurementSchema.getMeasurements();
    for (String measurement : measurements) {
      valueTimeseriesMetadataList.add(timeseriesMetadataCache.get(measurement));
    }

    VectorTimeSeriesMetadata vectorTimeSeriesMetadata =
        new VectorTimeSeriesMetadata(timeseriesMetadata, valueTimeseriesMetadataList);
    List<IChunkMetadata> chunkMetadataList =
        measurementChunkMetadataList.computeIfAbsent(
            vectorMeasurementSchema, m -> new ArrayList<>());
    chunkMetadataList.addAll(vectorTimeSeriesMetadata.loadChunkMetadataList());

    return measurements.length + 1;
  }
}
