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

import org.apache.iotdb.db.engine.memtable.AlignedWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.fill.AlignedLastPointReader;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.AlignedSeriesReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * VectorPartialPath represents many fullPaths of aligned timeseries. In the AlignedPath, the nodes
 * in PartialPath is deviceId e.g. VectorPartialPath nodes=root.sg1.alignedD1 measurementList=[s1,
 * s2]
 */
public class AlignedPath extends PartialPath {

  private static final Logger logger = LoggerFactory.getLogger(AlignedPath.class);

  // todo improve vector implementation by remove this placeholder
  public static final String VECTOR_PLACEHOLDER = "";

  private List<String> measurementList;
  private List<IMeasurementSchema> schemaList;

  public AlignedPath() {}

  public AlignedPath(String vectorPath, List<String> subSensorsList) throws IllegalPathException {
    super(vectorPath);
    this.measurementList = subSensorsList;
  }

  public AlignedPath(
      String vectorPath, List<String> measurementList, List<IMeasurementSchema> schemaList)
      throws IllegalPathException {
    super(vectorPath);
    this.measurementList = measurementList;
    this.schemaList = schemaList;
  }

  public AlignedPath(String vectorPath, String subSensor) throws IllegalPathException {
    super(vectorPath);
    measurementList = new ArrayList<>();
    measurementList.add(subSensor);
  }

  public AlignedPath(PartialPath vectorPath, String subSensor) {
    super(vectorPath.getNodes());
    measurementList = new ArrayList<>();
    measurementList.add(subSensor);
  }

  public AlignedPath(MeasurementPath path) {
    super(path.getDevicePath().getNodes());
    measurementList = new ArrayList<>();
    measurementList.add(path.getMeasurement());
    schemaList = new ArrayList<>();
    schemaList.add(path.getMeasurementSchema());
  }

  @Override
  public String getDevice() {
    return getFullPath();
  }

  @Override
  public String getMeasurement() {
    throw new UnsupportedOperationException("AlignedPath doesn't have measurement name!");
  }

  public List<String> getMeasurementList() {
    return measurementList;
  }

  public String getMeasurement(int index) {
    return measurementList.get(index);
  }

  public PartialPath getPathWithMeasurement(int index) {
    return new PartialPath(nodes).concatNode(measurementList.get(index));
  }

  public void setMeasurementList(List<String> measurementList) {
    this.measurementList = measurementList;
  }

  public void addMeasurement(List<String> measurements) {
    this.measurementList.addAll(measurements);
  }

  public void addMeasurement(MeasurementPath measurementPath) {
    if (measurementList == null) {
      measurementList = new ArrayList<>();
    }
    measurementList.add(measurementPath.getMeasurement());

    if (schemaList == null) {
      schemaList = new ArrayList<>();
    }
    schemaList.add(measurementPath.getMeasurementSchema());
  }

  public List<IMeasurementSchema> getSchemaList() {
    return this.schemaList == null ? Collections.emptyList() : this.schemaList;
  }

  public VectorMeasurementSchema getMeasurementSchema() {
    TSDataType[] types = new TSDataType[measurementList.size()];
    TSEncoding[] encodings = new TSEncoding[measurementList.size()];

    for (int i = 0; i < measurementList.size(); i++) {
      types[i] = schemaList.get(i).getType();
      encodings[i] = schemaList.get(i).getEncodingType();
    }
    String[] array = new String[measurementList.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = measurementList.get(i);
    }
    return new VectorMeasurementSchema(
        VECTOR_PLACEHOLDER, array, types, encodings, schemaList.get(0).getCompressor());
  }

  public TSDataType getSeriesType() {
    return TSDataType.VECTOR;
  }

  @Override
  public PartialPath copy() {
    AlignedPath result = new AlignedPath();
    result.nodes = nodes;
    result.fullPath = fullPath;
    result.device = device;
    result.measurementList = new ArrayList<>(measurementList);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AlignedPath that = (AlignedPath) o;
    return Objects.equals(measurementList, that.measurementList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), measurementList);
  }

  @Override
  public AlignedLastPointReader createLastPointReader(
      TSDataType dataType,
      Set<String> deviceMeasurements,
      QueryContext context,
      QueryDataSource dataSource,
      long queryTime,
      Filter timeFilter) {
    return new AlignedLastPointReader(
        this, dataType, deviceMeasurements, context, dataSource, queryTime, timeFilter);
  }

  @Override
  public AlignedSeriesReader createSeriesReader(
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    return new AlignedSeriesReader(
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

  @Override
  @TestOnly
  public AlignedSeriesReader createSeriesReader(
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      List<TsFileResource> seqFileResource,
      List<TsFileResource> unseqFileResource,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    return new AlignedSeriesReader(
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
  public TsFileResource createTsFileResource(
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<IChunkMetadata> chunkMetadataList,
      TsFileResource originTsFileResource)
      throws IOException {
    TsFileResource tsFileResource =
        new TsFileResource(readOnlyMemChunk, chunkMetadataList, originTsFileResource);
    tsFileResource.setTimeSeriesMetadata(
        generateTimeSeriesMetadata(readOnlyMemChunk, chunkMetadataList));
    return tsFileResource;
  }

  /**
   * Because the unclosed tsfile don't have TimeSeriesMetadata and memtables in the memory don't
   * have chunkMetadata, but query will use these, so we need to generate it for them.
   */
  private AlignedTimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList)
      throws IOException {
    TimeseriesMetadata timeTimeSeriesMetadata = new TimeseriesMetadata();
    timeTimeSeriesMetadata.setOffsetOfChunkMetaDataList(-1);
    timeTimeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);
    timeTimeSeriesMetadata.setMeasurementId("");
    timeTimeSeriesMetadata.setTSDataType(TSDataType.VECTOR);

    Statistics<? extends Serializable> timeStatistics =
        Statistics.getStatsByType(timeTimeSeriesMetadata.getTSDataType());

    // init each value time series meta
    List<TimeseriesMetadata> valueTimeSeriesMetadataList = new ArrayList<>();
    for (IMeasurementSchema valueChunkMetadata : schemaList) {
      TimeseriesMetadata valueMetadata = new TimeseriesMetadata();
      valueMetadata.setOffsetOfChunkMetaDataList(-1);
      valueMetadata.setDataSizeOfChunkMetaDataList(-1);
      valueMetadata.setMeasurementId(valueChunkMetadata.getMeasurementId());
      valueMetadata.setTSDataType(valueChunkMetadata.getType());
      valueMetadata.setStatistics(Statistics.getStatsByType(valueChunkMetadata.getType()));
      valueTimeSeriesMetadataList.add(valueMetadata);
    }

    boolean[] exist = new boolean[schemaList.size()];
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) chunkMetadata;
      timeStatistics.mergeStatistics(alignedChunkMetadata.getTimeChunkMetadata().getStatistics());
      for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
        if (alignedChunkMetadata.getValueChunkMetadataList().get(i) != null) {
          exist[i] = true;
          valueTimeSeriesMetadataList
              .get(i)
              .getStatistics()
              .mergeStatistics(
                  alignedChunkMetadata.getValueChunkMetadataList().get(i).getStatistics());
        }
      }
    }

    for (ReadOnlyMemChunk memChunk : readOnlyMemChunk) {
      if (!memChunk.isEmpty()) {
        AlignedChunkMetadata alignedChunkMetadata =
            (AlignedChunkMetadata) memChunk.getChunkMetaData();
        timeStatistics.mergeStatistics(alignedChunkMetadata.getTimeChunkMetadata().getStatistics());
        for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
          if (alignedChunkMetadata.getValueChunkMetadataList().get(i) != null) {
            exist[i] = true;
            valueTimeSeriesMetadataList
                .get(i)
                .getStatistics()
                .mergeStatistics(
                    alignedChunkMetadata.getValueChunkMetadataList().get(i).getStatistics());
          }
        }
      }
    }
    timeTimeSeriesMetadata.setStatistics(timeStatistics);

    for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
      if (!exist[i]) {
        valueTimeSeriesMetadataList.set(i, null);
      }
    }

    return new AlignedTimeSeriesMetadata(timeTimeSeriesMetadata, valueTimeSeriesMetadataList);
  }

  @Override
  public ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      Map<String, Map<String, IWritableMemChunk>> memTableMap, List<TimeRange> deletionList)
      throws QueryProcessException, IOException {
    // check If memtable contains this path
    if (!memTableMap.containsKey(getDevice())) {
      return null;
    }
    AlignedWritableMemChunk alignedMemChunk =
        ((AlignedWritableMemChunk) memTableMap.get(getDevice()).get(VECTOR_PLACEHOLDER));
    boolean containsMeasurement = false;
    for (String measurement : measurementList) {
      if (alignedMemChunk.containsMeasurement(measurement)) {
        containsMeasurement = true;
        break;
      }
    }
    if (!containsMeasurement) {
      return null;
    }
    // get sorted tv list is synchronized so different query can get right sorted list reference
    TVList alignedTvListCopy = alignedMemChunk.getSortedTvListForQuery(schemaList);
    int curSize = alignedTvListCopy.size();
    return new AlignedReadOnlyMemChunk(
        getMeasurementSchema(), alignedTvListCopy, curSize, deletionList);
  }

  @Override
  public List<IChunkMetadata> getVisibleMetadataListFromWriter(
      RestorableTsFileIOWriter writer, TsFileResource tsFileResource, QueryContext context) {
    ModificationFile modificationFile = tsFileResource.getModFile();
    List<List<Modification>> modifications = context.getPathModifications(modificationFile, this);

    List<AlignedChunkMetadata> chunkMetadataList = new ArrayList<>();
    List<ChunkMetadata> timeChunkMetadataList =
        writer.getVisibleMetadataList(getDevice(), "", getSeriesType());
    List<List<ChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
    for (int i = 0; i < measurementList.size(); i++) {
      valueChunkMetadataList.add(
          writer.getVisibleMetadataList(
              getDevice(), measurementList.get(i), schemaList.get(i).getType()));
    }

    for (int i = 0; i < timeChunkMetadataList.size(); i++) {
      List<IChunkMetadata> valueChunkMetadata = new ArrayList<>();
      // if all the sub sensors doesn't exist, it will be false
      boolean exits = false;
      for (List<ChunkMetadata> chunkMetadata : valueChunkMetadataList) {
        boolean currentExist = i < chunkMetadata.size();
        exits = (exits || currentExist);
        valueChunkMetadata.add(currentExist ? chunkMetadata.get(i) : null);
      }
      if (exits) {
        chunkMetadataList.add(
            new AlignedChunkMetadata(timeChunkMetadataList.get(i), valueChunkMetadata));
      }
    }

    QueryUtils.modifyAlignedChunkMetaData(chunkMetadataList, modifications);
    chunkMetadataList.removeIf(context::chunkNotSatisfy);
    return new ArrayList<>(chunkMetadataList);
  }

  @Override
  public int getColumnNum() {
    return measurementList.size();
  }

  @Override
  public AlignedPath clone() {
    AlignedPath alignedPath = null;
    try {
      alignedPath =
          new AlignedPath(
              this.getDevice(),
              new ArrayList<>(this.measurementList),
              new ArrayList<>(this.schemaList));
    } catch (IllegalPathException e) {
      logger.warn("path is illegal: {}", this.getFullPath(), e);
    }
    return alignedPath;
  }
}
