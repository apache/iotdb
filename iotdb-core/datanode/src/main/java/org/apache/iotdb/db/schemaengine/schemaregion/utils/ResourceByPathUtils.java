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
package org.apache.iotdb.db.schemaengine.schemaregion.utils;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.path.AlignedPath.VECTOR_PLACEHOLDER;

/**
 * Obtain required resources through path, such as readers and writers and etc. AlignedPath and
 * MeasurementPath have different implementations, and the default PartialPath should not use it.
 */
public abstract class ResourceByPathUtils {

  public static ResourceByPathUtils getResourceInstance(IFullPath path) {
    if (path instanceof AlignedFullPath) {
      return new AlignedResourceByPathUtils(path);
    } else if (path instanceof NonAlignedFullPath) {
      return new MeasurementResourceByPathUtils(path);
    }
    throw new UnsupportedOperationException("Should call exact sub class!");
  }

  public abstract ITimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList)
      throws IOException;

  public abstract ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      QueryContext context,
      IMemTable memTable,
      List<Pair<Modification, IMemTable>> modsToMemtable,
      long timeLowerBound)
      throws QueryProcessException, IOException;

  public abstract List<IChunkMetadata> getVisibleMetadataListFromWriter(
      RestorableTsFileIOWriter writer, TsFileResource tsFileResource, QueryContext context);

  /** get modifications from a memtable. */
  protected List<Modification> getModificationsForMemtable(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable) {
    List<Modification> modifications = new ArrayList<>();
    boolean foundMemtable = false;
    for (Pair<Modification, IMemTable> entry : modsToMemtable) {
      if (foundMemtable || entry.right.equals(memTable)) {
        modifications.add(entry.left);
        foundMemtable = true;
      }
    }
    return modifications;
  }
}

class AlignedResourceByPathUtils extends ResourceByPathUtils {

  AlignedFullPath alignedFullPath;

  public AlignedResourceByPathUtils(IFullPath fullPath) {
    this.alignedFullPath = (AlignedFullPath) fullPath;
  }

  /**
   * Because the unclosed tsfile don't have TimeSeriesMetadata and memtables in the memory don't
   * have chunkMetadata, but query will use these, so we need to generate it for them.
   */
  @Override
  public AlignedTimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList) {
    TimeseriesMetadata timeTimeSeriesMetadata = new TimeseriesMetadata();
    timeTimeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);
    timeTimeSeriesMetadata.setMeasurementId("");
    timeTimeSeriesMetadata.setTsDataType(TSDataType.VECTOR);

    Statistics<? extends Serializable> timeStatistics =
        Statistics.getStatsByType(timeTimeSeriesMetadata.getTsDataType());

    // init each value time series meta
    List<TimeseriesMetadata> valueTimeSeriesMetadataList = new ArrayList<>();
    for (IMeasurementSchema valueChunkMetadata : (alignedFullPath.getSchemaList())) {
      TimeseriesMetadata valueMetadata = new TimeseriesMetadata();
      valueMetadata.setDataSizeOfChunkMetaDataList(-1);
      valueMetadata.setMeasurementId(valueChunkMetadata.getMeasurementId());
      valueMetadata.setTsDataType(valueChunkMetadata.getType());
      valueMetadata.setStatistics(Statistics.getStatsByType(valueChunkMetadata.getType()));
      valueTimeSeriesMetadataList.add(valueMetadata);
    }

    boolean[] exist = new boolean[alignedFullPath.getSchemaList().size()];
    boolean modified = false;
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) chunkMetadata;
      modified = (modified || alignedChunkMetadata.isModified());
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
    timeTimeSeriesMetadata.setModified(modified);

    for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
      if (!exist[i]) {
        valueTimeSeriesMetadataList.set(i, null);
      }
    }

    return new AlignedTimeSeriesMetadata(timeTimeSeriesMetadata, valueTimeSeriesMetadataList);
  }

  @Override
  public ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      QueryContext context,
      IMemTable memTable,
      List<Pair<Modification, IMemTable>> modsToMemtable,
      long timeLowerBound)
      throws QueryProcessException, IOException {
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
    IDeviceID deviceID = alignedFullPath.getDeviceId();

    // check If memtable contains this path
    if (!memTableMap.containsKey(deviceID)) {
      return null;
    }
    AlignedWritableMemChunk alignedMemChunk =
        ((AlignedWritableMemChunkGroup) memTableMap.get(deviceID)).getAlignedMemChunk();
    boolean containsMeasurement = false;
    for (String measurement : alignedFullPath.getMeasurementList()) {
      if (alignedMemChunk.containsMeasurement(measurement)) {
        containsMeasurement = true;
        break;
      }
    }
    if (!containsMeasurement) {
      return null;
    }
    // get sorted tv list is synchronized so different query can get right sorted list reference
    TVList alignedTvListCopy =
        alignedMemChunk.getSortedTvListForQuery(alignedFullPath.getSchemaList());
    List<List<TimeRange>> deletionList = null;
    if (modsToMemtable != null) {
      deletionList = constructDeletionList(memTable, modsToMemtable, timeLowerBound);
    }
    return new AlignedReadOnlyMemChunk(
        context, getMeasurementSchema(), alignedTvListCopy, deletionList);
  }

  public VectorMeasurementSchema getMeasurementSchema() {
    List<String> measurementList = alignedFullPath.getMeasurementList();
    TSDataType[] types = new TSDataType[measurementList.size()];
    TSEncoding[] encodings = new TSEncoding[measurementList.size()];

    for (int i = 0; i < measurementList.size(); i++) {
      types[i] = alignedFullPath.getSchemaList().get(i).getType();
      encodings[i] = alignedFullPath.getSchemaList().get(i).getEncodingType();
    }
    String[] array = new String[measurementList.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = measurementList.get(i);
    }
    return new VectorMeasurementSchema(
        VECTOR_PLACEHOLDER,
        array,
        types,
        encodings,
        alignedFullPath.getSchemaList().get(0).getCompressor());
  }

  private List<List<TimeRange>> constructDeletionList(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound) {
    List<List<TimeRange>> deletionList = new ArrayList<>();
    for (String measurement : alignedFullPath.getMeasurementList()) {
      List<TimeRange> columnDeletionList = new ArrayList<>();
      columnDeletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
      for (Modification modification : getModificationsForMemtable(memTable, modsToMemtable)) {
        if (modification instanceof Deletion) {
          Deletion deletion = (Deletion) modification;
          if (deletion.getPath().matchFullPath(alignedFullPath.getDeviceId(), measurement)
              && deletion.getEndTime() > timeLowerBound) {
            long lowerBound = Math.max(deletion.getStartTime(), timeLowerBound);
            columnDeletionList.add(new TimeRange(lowerBound, deletion.getEndTime()));
          }
        }
      }
      deletionList.add(TimeRange.sortAndMerge(columnDeletionList));
    }
    return deletionList;
  }

  @Override
  public List<IChunkMetadata> getVisibleMetadataListFromWriter(
      RestorableTsFileIOWriter writer, TsFileResource tsFileResource, QueryContext context) {
    List<List<Modification>> modifications =
        context.getPathModifications(
            tsFileResource, alignedFullPath.getDeviceId(), alignedFullPath.getMeasurementList());

    List<AlignedChunkMetadata> chunkMetadataList = new ArrayList<>();
    List<ChunkMetadata> timeChunkMetadataList =
        writer.getVisibleMetadataList(
            alignedFullPath.getDeviceId(), AlignedFullPath.VECTOR_PLACEHOLDER, TSDataType.VECTOR);
    List<List<ChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
    for (int i = 0; i < alignedFullPath.getMeasurementList().size(); i++) {
      valueChunkMetadataList.add(
          writer.getVisibleMetadataList(
              alignedFullPath.getDeviceId(),
              alignedFullPath.getMeasurementList().get(i),
              alignedFullPath.getSchemaList().get(i).getType()));
    }

    for (int i = 0; i < timeChunkMetadataList.size(); i++) {
      // only need time column
      if (alignedFullPath.getMeasurementList().isEmpty()) {
        chunkMetadataList.add(
            new AlignedChunkMetadata(timeChunkMetadataList.get(i), Collections.emptyList()));
      } else {
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
    }

    ModificationUtils.modifyAlignedChunkMetaData(chunkMetadataList, modifications);
    chunkMetadataList.removeIf(context::chunkNotSatisfy);
    return new ArrayList<>(chunkMetadataList);
  }
}

class MeasurementResourceByPathUtils extends ResourceByPathUtils {

  NonAlignedFullPath fullPath;

  protected MeasurementResourceByPathUtils(IFullPath fullPath) {
    this.fullPath = (NonAlignedFullPath) fullPath;
  }

  /**
   * Because the unclosed tsfile don't have TimeSeriesMetadata and memtables in the memory don't
   * have chunkMetadata, but query will use these, so we need to generate it for them.
   */
  @Override
  public ITimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList) {
    TimeseriesMetadata timeSeriesMetadata = new TimeseriesMetadata();
    timeSeriesMetadata.setMeasurementId(fullPath.getMeasurementSchema().getMeasurementId());
    timeSeriesMetadata.setTsDataType(fullPath.getMeasurementSchema().getType());
    timeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);

    Statistics<? extends Serializable> seriesStatistics =
        Statistics.getStatsByType(timeSeriesMetadata.getTsDataType());
    // flush chunkMetadataList one by one
    boolean isModified = false;
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      isModified = (isModified || chunkMetadata.isModified());
      seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
    }

    for (ReadOnlyMemChunk memChunk : readOnlyMemChunk) {
      if (!memChunk.isEmpty()) {
        seriesStatistics.mergeStatistics(memChunk.getChunkMetaData().getStatistics());
      }
    }
    timeSeriesMetadata.setStatistics(seriesStatistics);
    timeSeriesMetadata.setModified(isModified);
    return timeSeriesMetadata;
  }

  @Override
  public ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      QueryContext context,
      IMemTable memTable,
      List<Pair<Modification, IMemTable>> modsToMemtable,
      long timeLowerBound)
      throws QueryProcessException, IOException {
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
    IDeviceID deviceID = fullPath.getDeviceId();
    // check If Memtable Contains this path
    if (!memTableMap.containsKey(deviceID)
        || !memTableMap.get(deviceID).contains(fullPath.getMeasurement())) {
      return null;
    }
    IWritableMemChunk memChunk =
        memTableMap.get(deviceID).getMemChunkMap().get(fullPath.getMeasurement());
    // get sorted tv list is synchronized so different query can get right sorted list reference
    TVList chunkCopy = memChunk.getSortedTvListForQuery();
    List<TimeRange> deletionList = null;
    if (modsToMemtable != null) {
      deletionList = constructDeletionList(memTable, modsToMemtable, timeLowerBound);
    }
    return new ReadOnlyMemChunk(
        context,
        fullPath.getMeasurement(),
        fullPath.getMeasurementSchema().getType(),
        fullPath.getMeasurementSchema().getEncodingType(),
        chunkCopy,
        fullPath.getMeasurementSchema().getProps(),
        deletionList);
  }
  /**
   * construct a deletion list from a memtable.
   *
   * @param memTable memtable
   * @param timeLowerBound time watermark
   */
  private List<TimeRange> constructDeletionList(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound) {
    List<TimeRange> deletionList = new ArrayList<>();
    deletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
    for (Modification modification : getModificationsForMemtable(memTable, modsToMemtable)) {
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getPath().matchFullPath(fullPath.getDeviceId(), fullPath.getMeasurement())
            && deletion.getEndTime() > timeLowerBound) {
          long lowerBound = Math.max(deletion.getStartTime(), timeLowerBound);
          deletionList.add(new TimeRange(lowerBound, deletion.getEndTime()));
        }
      }
    }
    return TimeRange.sortAndMerge(deletionList);
  }
  /** get modifications from a memtable. */
  @Override
  protected List<Modification> getModificationsForMemtable(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable) {
    List<Modification> modifications = new ArrayList<>();
    boolean foundMemtable = false;
    for (Pair<Modification, IMemTable> entry : modsToMemtable) {
      if (foundMemtable || entry.right.equals(memTable)) {
        modifications.add(entry.left);
        foundMemtable = true;
      }
    }
    return modifications;
  }

  @Override
  public List<IChunkMetadata> getVisibleMetadataListFromWriter(
      RestorableTsFileIOWriter writer, TsFileResource tsFileResource, QueryContext context) {
    List<Modification> modifications =
        context.getPathModifications(
            tsFileResource, fullPath.getDeviceId(), fullPath.getMeasurement());

    List<IChunkMetadata> chunkMetadataList =
        new ArrayList<>(
            writer.getVisibleMetadataList(
                fullPath.getDeviceId(),
                fullPath.getMeasurement(),
                fullPath.getMeasurementSchema().getType()));

    ModificationUtils.modifyChunkMetaData(chunkMetadataList, modifications);
    chunkMetadataList.removeIf(context::chunkNotSatisfy);
    return chunkMetadataList;
  }
}
