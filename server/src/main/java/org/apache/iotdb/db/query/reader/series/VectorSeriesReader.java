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
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class VectorSeriesReader extends SeriesReader {

  private final VectorPartialPath vectorPartialPath;

  public VectorSeriesReader(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    super(
        seriesPath,
        allSensors,
        dataType,
        context,
        dataSource,
        timeFilter,
        valueFilter,
        fileFilter,
        ascending);
    this.allSensors.add(seriesPath.getMeasurement());
    this.vectorPartialPath = (VectorPartialPath) seriesPath;
  }

  @TestOnly
  VectorSeriesReader(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      List<TsFileResource> seqFileResource,
      List<TsFileResource> unseqFileResource,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    super(
        seriesPath,
        allSensors,
        dataType,
        context,
        seqFileResource,
        unseqFileResource,
        timeFilter,
        valueFilter,
        ascending);
    this.allSensors.add(seriesPath.getMeasurement());
    this.vectorPartialPath = (VectorPartialPath) seriesPath;
  }

  @Override
  protected void unpackSeqTsFileResource() throws IOException {
    TsFileResource resource = orderUtils.getNextSeqFileResource(seqFileResource, true);
    TimeseriesMetadata timeseriesMetadata =
        FileLoaderUtils.loadTimeSeriesMetadata(
            resource, vectorPartialPath, context, getAnyFilter(), allSensors);
    if (timeseriesMetadata != null) {
      timeseriesMetadata.setSeq(true);
      List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();
      for (PartialPath subSensor : vectorPartialPath.getSubSensorsPathList()) {
        TimeseriesMetadata valueTimeSeriesMetadata =
            FileLoaderUtils.loadTimeSeriesMetadata(
                resource, subSensor, context, getAnyFilter(), allSensors);
        if (valueTimeSeriesMetadata == null) {
          throw new IOException("File doesn't contains value");
        }
        valueTimeSeriesMetadata.setSeq(true);
        valueTimeseriesMetadataList.add(valueTimeSeriesMetadata);
      }
      VectorTimeSeriesMetadata vectorTimeSeriesMetadata =
          new VectorTimeSeriesMetadata(timeseriesMetadata, valueTimeseriesMetadataList);
      seqTimeSeriesMetadata.add(vectorTimeSeriesMetadata);
    }
  }

  @Override
  protected void unpackUnseqTsFileResource() throws IOException {
    TsFileResource resource = unseqFileResource.remove(0);
    TimeseriesMetadata timeseriesMetadata =
        FileLoaderUtils.loadTimeSeriesMetadata(
            resource, vectorPartialPath, context, getAnyFilter(), allSensors);
    if (timeseriesMetadata != null) {
      timeseriesMetadata.setModified(true);
      timeseriesMetadata.setSeq(false);
      List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();
      for (PartialPath subSensor : vectorPartialPath.getSubSensorsPathList()) {
        TimeseriesMetadata valueTimeSeriesMetadata =
            FileLoaderUtils.loadTimeSeriesMetadata(
                resource, subSensor, context, getAnyFilter(), allSensors);
        if (valueTimeSeriesMetadata == null) {
          throw new IOException("File contains value");
        }
        timeseriesMetadata.setModified(true);
        valueTimeSeriesMetadata.setSeq(false);
        valueTimeseriesMetadataList.add(valueTimeSeriesMetadata);
      }
      VectorTimeSeriesMetadata vectorTimeSeriesMetadata =
          new VectorTimeSeriesMetadata(timeseriesMetadata, valueTimeseriesMetadataList);
      unSeqTimeSeriesMetadata.add(vectorTimeSeriesMetadata);
    }
  }
}
