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
package org.apache.iotdb.db.metadata.utils;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.memtable.AlignedWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.AlignedWritableMemChunkGroup;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.path.AlignedPath.VECTOR_PLACEHOLDER;

/**
 * Obtain required resources through path, such as readers and writers and etc. AlignedPath and
 * MeasurementPath have different implementations, and the default PartialPath should not use it.
 */
public abstract class ResourceByPathUtils {

  public static ResourceByPathUtils getResourceInstance(PartialPath path) {
    if (path instanceof AlignedPath) {
      return new AlignedResourceByPathUtils(path);
    } else if (path instanceof MeasurementPath) {
      return new MeasurementResourceByPathUtils(path);
    }
    throw new UnsupportedOperationException("Should call exact sub class!");
  }

  public abstract ITimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList)
      throws IOException;

  public abstract ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound)
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

  AlignedPath partialPath;

  public AlignedResourceByPathUtils(PartialPath partialPath) {
    this.partialPath = (AlignedPath) partialPath;
  }

  /**
   * Because the unclosed tsfile don't have TimeSeriesMetadata and memtables in the memory don't
   * have chunkMetadata, but query will use these, so we need to generate it for them.
   */
  @Override
  public AlignedTimeSeriesMetadata generateTimeSeriesMetadata(
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
    for (IMeasurementSchema valueChunkMetadata : (partialPath.getSchemaList())) {
      TimeseriesMetadata valueMetadata = new TimeseriesMetadata();
      valueMetadata.setOffsetOfChunkMetaDataList(-1);
      valueMetadata.setDataSizeOfChunkMetaDataList(-1);
      valueMetadata.setMeasurementId(valueChunkMetadata.getMeasurementId());
      valueMetadata.setTSDataType(valueChunkMetadata.getType());
      valueMetadata.setStatistics(Statistics.getStatsByType(valueChunkMetadata.getType()));
      valueTimeSeriesMetadataList.add(valueMetadata);
    }

    boolean[] exist = new boolean[partialPath.getSchemaList().size()];
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
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound)
      throws QueryProcessException, IOException {
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(partialPath);

    // check If memtable contains this path
    if (!memTableMap.containsKey(deviceID)) {
      return null;
    }
    AlignedWritableMemChunk alignedMemChunk =
        ((AlignedWritableMemChunkGroup) memTableMap.get(deviceID)).getAlignedMemChunk();
    boolean containsMeasurement = false;
    for (String measurement : partialPath.getMeasurementList()) {
      if (alignedMemChunk.containsMeasurement(measurement)) {
        containsMeasurement = true;
        break;
      }
    }
    if (!containsMeasurement) {
      return null;
    }
    // get sorted tv list is synchronized so different query can get right sorted list reference
    TVList alignedTvListCopy = alignedMemChunk.getSortedTvListForQuery(partialPath.getSchemaList());
    List<List<TimeRange>> deletionList = null;
    if (modsToMemtable != null) {
      deletionList = constructDeletionList(memTable, modsToMemtable, timeLowerBound);
    }
    return new AlignedReadOnlyMemChunk(getMeasurementSchema(), alignedTvListCopy, deletionList);
  }

  public VectorMeasurementSchema getMeasurementSchema() {
    List<String> measurementList = partialPath.getMeasurementList();
    TSDataType[] types = new TSDataType[measurementList.size()];
    TSEncoding[] encodings = new TSEncoding[measurementList.size()];

    for (int i = 0; i < measurementList.size(); i++) {
      types[i] = partialPath.getSchemaList().get(i).getType();
      encodings[i] = partialPath.getSchemaList().get(i).getEncodingType();
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
        partialPath.getSchemaList().get(0).getCompressor());
  }

  private List<List<TimeRange>> constructDeletionList(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound) {
    List<List<TimeRange>> deletionList = new ArrayList<>();
    for (String measurement : partialPath.getMeasurementList()) {
      List<TimeRange> columnDeletionList = new ArrayList<>();
      columnDeletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
      for (Modification modification : getModificationsForMemtable(memTable, modsToMemtable)) {
        if (modification instanceof Deletion) {
          Deletion deletion = (Deletion) modification;
          PartialPath fullPath = partialPath.concatNode(measurement);
          if (deletion.getPath().matchFullPath(fullPath)
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
    ModificationFile modificationFile = tsFileResource.getModFile();
    List<List<Modification>> modifications =
        context.getPathModifications(modificationFile, partialPath);

    List<AlignedChunkMetadata> chunkMetadataList = new ArrayList<>();
    List<ChunkMetadata> timeChunkMetadataList =
        writer.getVisibleMetadataList(partialPath.getDevice(), "", partialPath.getSeriesType());
    List<List<ChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
    for (int i = 0; i < partialPath.getMeasurementList().size(); i++) {
      valueChunkMetadataList.add(
          writer.getVisibleMetadataList(
              partialPath.getDevice(),
              partialPath.getMeasurementList().get(i),
              partialPath.getSchemaList().get(i).getType()));
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

    ModificationUtils.modifyAlignedChunkMetaData(chunkMetadataList, modifications);
    chunkMetadataList.removeIf(context::chunkNotSatisfy);
    return new ArrayList<>(chunkMetadataList);
  }
}

class MeasurementResourceByPathUtils extends ResourceByPathUtils {

  MeasurementPath partialPath;

  protected MeasurementResourceByPathUtils(PartialPath partialPath) {
    this.partialPath = (MeasurementPath) partialPath;
  }

  /**
   * Because the unclosed tsfile don't have TimeSeriesMetadata and memtables in the memory don't
   * have chunkMetadata, but query will use these, so we need to generate it for them.
   */
  @Override
  public ITimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList)
      throws IOException {
    TimeseriesMetadata timeSeriesMetadata = new TimeseriesMetadata();
    timeSeriesMetadata.setMeasurementId(partialPath.getMeasurementSchema().getMeasurementId());
    timeSeriesMetadata.setTSDataType(partialPath.getMeasurementSchema().getType());
    timeSeriesMetadata.setOffsetOfChunkMetaDataList(-1);
    timeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);

    Statistics<? extends Serializable> seriesStatistics =
        Statistics.getStatsByType(timeSeriesMetadata.getTSDataType());
    // flush chunkMetadataList one by one
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
    }

    for (ReadOnlyMemChunk memChunk : readOnlyMemChunk) {
      if (!memChunk.isEmpty()) {
        seriesStatistics.mergeStatistics(memChunk.getChunkMetaData().getStatistics());
      }
    }
    timeSeriesMetadata.setStatistics(seriesStatistics);
    return timeSeriesMetadata;
  }

  @Override
  public ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound)
      throws QueryProcessException, IOException {
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(partialPath.getDevicePath());
    // check If Memtable Contains this path
    if (!memTableMap.containsKey(deviceID)
        || !memTableMap.get(deviceID).contains(partialPath.getMeasurement())) {
      return null;
    }
    IWritableMemChunk memChunk =
        memTableMap.get(deviceID).getMemChunkMap().get(partialPath.getMeasurement());
    // get sorted tv list is synchronized so different query can get right sorted list reference
    TVList chunkCopy = memChunk.getSortedTvListForQuery();
    List<TimeRange> deletionList = null;
    if (modsToMemtable != null) {
      deletionList = constructDeletionList(memTable, modsToMemtable, timeLowerBound);
    }
    return new ReadOnlyMemChunk(
        partialPath.getMeasurement(),
        partialPath.getMeasurementSchema().getType(),
        partialPath.getMeasurementSchema().getEncodingType(),
        chunkCopy,
        partialPath.getMeasurementSchema().getProps(),
        deletionList);
  }
  /**
   * construct a deletion list from a memtable.
   *
   * @param memTable memtable
   * @param timeLowerBound time water mark
   */
  private List<TimeRange> constructDeletionList(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound) {
    List<TimeRange> deletionList = new ArrayList<>();
    deletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
    for (Modification modification : getModificationsForMemtable(memTable, modsToMemtable)) {
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getPath().matchFullPath(partialPath)
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
    ModificationFile modificationFile = tsFileResource.getModFile();
    List<Modification> modifications = context.getPathModifications(modificationFile, partialPath);

    List<IChunkMetadata> chunkMetadataList =
        new ArrayList<>(
            writer.getVisibleMetadataList(
                partialPath.getDevice(),
                partialPath.getMeasurement(),
                partialPath.getSeriesType()));

    ModificationUtils.modifyChunkMetaData(chunkMetadataList, modifications);
    chunkMetadataList.removeIf(context::chunkNotSatisfy);
    return chunkMetadataList;
  }
}
