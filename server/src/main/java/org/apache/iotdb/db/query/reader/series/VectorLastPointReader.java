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
package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class VectorLastPointReader extends LastPointReader {
  private final VectorPartialPath vectorPartialPath;

  public VectorLastPointReader(
      PartialPath seriesPath,
      TSDataType dataType,
      Set<String> deviceMeasurements,
      QueryContext context,
      QueryDataSource dataSource,
      long queryTime,
      Filter timeFilter)
      throws IOException {
    super(seriesPath, dataType, deviceMeasurements, context, dataSource, queryTime, timeFilter);
    vectorPartialPath = (VectorPartialPath) seriesPath;
  }

  /** Pick up and cache the last sequence TimeseriesMetadata that satisfies timeFilter */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void cacheLastFromSeqFiles() throws IOException {
    List<TsFileResource> seqFileResource = dataSource.getSeqResources();
    TimeseriesMetadata lastMetadata;
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      TsFileResource resource = seqFileResource.get(index);
      lastMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              resource, vectorPartialPath, context, timeFilter, deviceMeasurements);
      if (lastMetadata == null) {
        continue;
      }
      if (!lastMetadata.isModified()
          && endtimeContainedByTimeFilter(lastMetadata.getStatistics())) {
        List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();
        for (PartialPath subSensor : vectorPartialPath.getSubSensorsPathList()) {
          TimeseriesMetadata valueTimeSeriesMetadata =
              FileLoaderUtils.loadTimeSeriesMetadata(
                  resource, subSensor, context, timeFilter, deviceMeasurements);
          if (valueTimeSeriesMetadata == null) {
            throw new IOException("File doesn't contains value");
          }
          valueTimeseriesMetadataList.add(valueTimeSeriesMetadata);
        }
        VectorTimeSeriesMetadata vecMetadata =
            new VectorTimeSeriesMetadata(lastMetadata, valueTimeseriesMetadataList);
        cachedLastPair =
            new TimeValuePair(
                lastMetadata.getStatistics().getEndTime(), vecMetadata.getStatisticalLastValue());
      } else {
        cachedLastPair = getLastByUnpackingTimeseries(lastMetadata);
        if (cachedLastPair != null) {
          return;
        }
      }
    }
  }

  /** find the last TimeseriesMetadata in unseq files and unpack all overlapped unseq files */
  protected void UnpackOverlappedUnseqFiles(long lBoundTime) throws IOException {
    PriorityQueue<TsFileResource> unseqFileResource =
        sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());

    while (!unseqFileResource.isEmpty()
        && (lBoundTime <= unseqFileResource.peek().getEndTime(seriesPath.getDevice()))) {
      TsFileResource resource = unseqFileResource.poll();
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              resource, vectorPartialPath, context, timeFilter, deviceMeasurements);

      if (timeseriesMetadata == null
          || (!timeseriesMetadata.isModified()
              && timeseriesMetadata.getStatistics().getEndTime() < lBoundTime)) {
        continue;
      }

      if (!timeseriesMetadata.isModified()) {
        if (endtimeContainedByTimeFilter(timeseriesMetadata.getStatistics())) {
          lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getEndTime());
        } else {
          lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getStartTime());
        }
      }

      List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();
      for (PartialPath subSensor : vectorPartialPath.getSubSensorsPathList()) {
        TimeseriesMetadata valueTimeSeriesMetadata =
            FileLoaderUtils.loadTimeSeriesMetadata(
                resource, subSensor, context, timeFilter, deviceMeasurements);
        if (valueTimeSeriesMetadata == null) {
          throw new IOException("File doesn't contains value");
        }
        valueTimeseriesMetadataList.add(valueTimeSeriesMetadata);
      }
      VectorTimeSeriesMetadata vecMetadata =
          new VectorTimeSeriesMetadata(timeseriesMetadata, valueTimeseriesMetadataList);
      unseqTimeseriesMetadataList.add(vecMetadata);
    }
  }
}
