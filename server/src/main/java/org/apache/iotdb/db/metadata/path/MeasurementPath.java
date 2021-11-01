/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MeasurementPath extends PartialPath {

  private IMeasurementSchema measurementSchema;

  private boolean isUnderAlignedEntity = false;

  // alias of measurement, null pointer cannot be serialized in thrift so empty string is instead
  private String measurementAlias = "";

  public MeasurementPath() {}

  public MeasurementPath(PartialPath measurementPath) {
    super(measurementPath.getNodes());
  }

  public MeasurementPath(String device, String measurement, IMeasurementSchema measurementSchema)
      throws IllegalPathException {
    super(device, measurement);
    this.measurementSchema = measurementSchema;
  }

  public IMeasurementSchema getMeasurementSchema() {
    return measurementSchema;
  }

  public void setMeasurementSchema(IMeasurementSchema measurementSchema) {
    this.measurementSchema = measurementSchema;
  }

  public String getMeasurementAlias() {
    return measurementAlias;
  }

  public void setMeasurementAlias(String measurementAlias) {
    if (measurementAlias != null) {
      this.measurementAlias = measurementAlias;
    }
  }

  public boolean isMeasurementAliasExists() {
    return measurementAlias != null && !measurementAlias.isEmpty();
  }

  @Override
  public String getFullPathWithAlias() {
    return getDevice() + IoTDBConstant.PATH_SEPARATOR + measurementAlias;
  }

  public boolean isUnderAlignedEntity() {
    return isUnderAlignedEntity;
  }

  public void setUnderAlignedEntity(boolean underAlignedEntity) {
    isUnderAlignedEntity = underAlignedEntity;
  }

  public PartialPath copy() {
    MeasurementPath result = new MeasurementPath();
    result.nodes = nodes;
    result.fullPath = fullPath;
    result.device = device;
    result.measurementAlias = measurementAlias;
    return result;
  }

  public SeriesReader createSeriesReader(
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    return new SeriesReader(
        this,
        allSensors,
        dataType,
        context,
        dataSource,
        timeFilter,
        valueFilter,
        fileFilter,
        ascending);
  }

  @TestOnly
  public SeriesReader createSeriesReader(
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      List<TsFileResource> seqFileResource,
      List<TsFileResource> unseqFileResource,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    return new SeriesReader(
        this,
        allSensors,
        dataType,
        context,
        seqFileResource,
        unseqFileResource,
        timeFilter,
        valueFilter,
        ascending);
  }

  @Override
  public ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      Map<String, Map<String, IWritableMemChunk>> memTableMap, List<TimeRange> deletionList)
      throws QueryProcessException, IOException {
    // check If Memtable Contains this path
    if (!memTableMap.containsKey(getDevice())
        || !memTableMap.get(getDevice()).containsKey(getMeasurement())) {
      return null;
    }
    IWritableMemChunk memChunk = memTableMap.get(getDevice()).get(getMeasurement());
    // get sorted tv list is synchronized so different query can get right sorted list reference
    TVList chunkCopy = memChunk.getSortedTvListForQuery();
    int curSize = chunkCopy.size();
    return new ReadOnlyMemChunk(
        getMeasurement(),
        measurementSchema.getType(),
        measurementSchema.getEncodingType(),
        chunkCopy,
        measurementSchema.getProps(),
        curSize,
        deletionList);
  }
}
