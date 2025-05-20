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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
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
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.path.AlignedPath.VECTOR_PLACEHOLDER;

/**
 * Obtain required resources through path, such as readers and writers and etc. AlignedPath and
 * MeasurementPath have different implementations, and the default PartialPath should not use it.
 */
public abstract class ResourceByPathUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceByPathUtils.class);

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
      QueryContext context,
      IMemTable memTable,
      List<Pair<Modification, IMemTable>> modsToMemtable,
      long timeLowerBound,
      Filter globalTimeFilter)
      throws QueryProcessException, IOException;

  public abstract List<IChunkMetadata> getVisibleMetadataListFromWriter(
      RestorableTsFileIOWriter writer,
      TsFileResource tsFileResource,
      QueryContext context,
      long timeLowerBound);

  /**
   * Prepare the TVList references for the query. We remember TVLists' row count here and determine
   * whether the TVLists needs sorting later during operator execution based on it. It need not
   * protect sorted list. Sorted list is changed in the handover process of inserting, which holds
   * the data region write lock. At this moment, query thread holds the data region read lock.
   *
   * @param context query context
   * @param memChunk writable memchunk
   * @param isWorkMemTable in working or flushing memtable
   * @param globalTimeFilter global time filter
   * @return Map<TVList, Integer>
   */
  protected Map<TVList, Integer> prepareTvListMapForQuery(
      QueryContext context,
      IWritableMemChunk memChunk,
      boolean isWorkMemTable,
      Filter globalTimeFilter) {
    // should copy globalTimeFilter because GroupByMonthFilter is stateful
    Filter copyTimeFilter = null;
    if (globalTimeFilter != null) {
      copyTimeFilter = globalTimeFilter.copy();
    }

    Map<TVList, Integer> tvListQueryMap = new LinkedHashMap<>();
    // immutable sorted lists
    for (TVList tvList : memChunk.getSortedList()) {
      if (copyTimeFilter != null
          && !copyTimeFilter.satisfyStartEndTime(tvList.getMinTime(), tvList.getMaxTime())) {
        continue;
      }
      tvList.lockQueryList();
      try {
        LOGGER.debug(
            "Flushing/Working MemTable - add current query context to immutable TVList's query list");
        tvList.getQueryContextSet().add(context);
        tvListQueryMap.put(tvList, tvList.rowCount());
      } finally {
        tvList.unlockQueryList();
      }
    }

    // mutable tvlist
    TVList list = memChunk.getWorkingTVList();
    TVList cloneList = null;
    list.lockQueryList();
    try {
      if (copyTimeFilter != null
          && !copyTimeFilter.satisfyStartEndTime(list.getMinTime(), list.getMaxTime())) {
        return tvListQueryMap;
      }

      if (!isWorkMemTable) {
        LOGGER.debug(
            "Flushing MemTable - add current query context to mutable TVList's query list");
        list.getQueryContextSet().add(context);
        tvListQueryMap.put(list, list.rowCount());
      } else {
        if (list.isSorted() || list.getQueryContextSet().isEmpty()) {
          LOGGER.debug(
              "Working MemTable - add current query context to mutable TVList's query list when it's sorted or no other query on it");
          list.getQueryContextSet().add(context);
          tvListQueryMap.put(list, list.rowCount());
        } else {
          /*
           * +----------------------+
           * |      MemTable        |
           * |                      |
           * |    +------------+    |          +-----------------+
           * |    |   TVList   |<---+--+   +---+  Previous Query |
           * |    +-----^------+    |  |   |   +-----------------+
           * |          |           |  |   |
           * +----------+-----------+  |   |   +----------------+
           *            | Clone        +---+---+  Current Query |
           *      +-----+------+           |   +----------------+
           *      |   TVList   | <---------+
           *      +------------+
           */
          LOGGER.debug(
              "Working MemTable - clone mutable TVList and replace old TVList in working MemTable");
          QueryContext firstQuery = list.getQueryContextSet().iterator().next();
          // reserve query memory
          if (firstQuery instanceof FragmentInstanceContext) {
            MemoryReservationManager memoryReservationManager =
                ((FragmentInstanceContext) firstQuery).getMemoryReservationContext();
            memoryReservationManager.reserveMemoryCumulatively(list.calculateRamSize());
          }
          list.setOwnerQuery(firstQuery);

          // clone TVList
          cloneList = list.clone();
          cloneList.getQueryContextSet().add(context);
          tvListQueryMap.put(cloneList, cloneList.rowCount());
        }
      }
    } finally {
      list.unlockQueryList();
    }
    if (cloneList != null) {
      memChunk.setWorkingTVList(cloneList);
    }
    return tvListQueryMap;
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
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList) {
    TimeseriesMetadata timeTimeSeriesMetadata = new TimeseriesMetadata();
    timeTimeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);
    timeTimeSeriesMetadata.setMeasurementId("");
    timeTimeSeriesMetadata.setTsDataType(TSDataType.VECTOR);

    Statistics<? extends Serializable> timeStatistics =
        Statistics.getStatsByType(timeTimeSeriesMetadata.getTsDataType());

    // init each value time series meta
    List<TimeseriesMetadata> valueTimeSeriesMetadataList = new ArrayList<>();
    for (IMeasurementSchema valueChunkMetadata : (partialPath.getSchemaList())) {
      TimeseriesMetadata valueMetadata = new TimeseriesMetadata();
      valueMetadata.setDataSizeOfChunkMetaDataList(-1);
      valueMetadata.setMeasurementId(valueChunkMetadata.getMeasurementId());
      valueMetadata.setTsDataType(valueChunkMetadata.getType());
      valueMetadata.setStatistics(Statistics.getStatsByType(valueChunkMetadata.getType()));
      valueTimeSeriesMetadataList.add(valueMetadata);
    }

    boolean[] exist = new boolean[partialPath.getSchemaList().size()];
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
        memChunk.sortTvLists();
        memChunk.initChunkMetaFromTvLists();
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
      long timeLowerBound,
      Filter globalTimeFilter)
      throws QueryProcessException {
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

    // prepare AlignedTVList for query. It should clone TVList if necessary.
    Map<TVList, Integer> alignedTvListQueryMap =
        prepareTvListMapForQuery(
            context, alignedMemChunk, modsToMemtable == null, globalTimeFilter);

    // column index list for the query
    List<Integer> columnIndexList =
        alignedMemChunk.buildColumnIndexList(partialPath.getSchemaList());

    List<List<TimeRange>> deletionList = null;
    if (modsToMemtable != null) {
      deletionList =
          ModificationUtils.constructDeletionList(
              partialPath, memTable, modsToMemtable, timeLowerBound);
    }
    return new AlignedReadOnlyMemChunk(
        context, columnIndexList, getMeasurementSchema(), alignedTvListQueryMap, deletionList);
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

  @Override
  public List<IChunkMetadata> getVisibleMetadataListFromWriter(
      RestorableTsFileIOWriter writer,
      TsFileResource tsFileResource,
      QueryContext context,
      long timeLowerBound) {
    List<List<Modification>> modifications =
        context.getPathModifications(tsFileResource, partialPath);

    List<AlignedChunkMetadata> chunkMetadataList = new ArrayList<>();
    List<ChunkMetadata> timeChunkMetadataList =
        writer.getVisibleMetadataList(partialPath.getIDeviceID(), "", partialPath.getSeriesType());
    List<List<ChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
    for (int i = 0; i < partialPath.getMeasurementList().size(); i++) {
      valueChunkMetadataList.add(
          writer.getVisibleMetadataList(
              partialPath.getIDeviceID(),
              partialPath.getMeasurementList().get(i),
              partialPath.getSchemaList().get(i).getType()));
    }

    for (int i = 0; i < timeChunkMetadataList.size(); i++) {
      // only need time column
      if (partialPath.getMeasurementList().isEmpty()) {
        chunkMetadataList.add(
            new AlignedChunkMetadata(timeChunkMetadataList.get(i), Collections.emptyList()));
      } else {
        List<IChunkMetadata> valueChunkMetadata = new ArrayList<>();
        // if all the sub sensors doesn't exist, it will be false
        boolean exits = false;
        for (List<ChunkMetadata> chunkMetadata : valueChunkMetadataList) {
          boolean currentExist =
              i < chunkMetadata.size() && chunkMetadata.get(i).getNumOfPoints() > 0;
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
    chunkMetadataList.removeIf(x -> x.getEndTime() < timeLowerBound);
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
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList) {
    TimeseriesMetadata timeSeriesMetadata = new TimeseriesMetadata();
    timeSeriesMetadata.setMeasurementId(partialPath.getMeasurementSchema().getMeasurementId());
    timeSeriesMetadata.setTsDataType(partialPath.getMeasurementSchema().getType());
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
        memChunk.sortTvLists();
        memChunk.initChunkMetaFromTvLists();
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
      long timeLowerBound,
      Filter globalTimeFilter)
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
    // prepare TVList for query. It should clone TVList if necessary.
    Map<TVList, Integer> tvListQueryMap =
        prepareTvListMapForQuery(context, memChunk, modsToMemtable == null, globalTimeFilter);
    List<TimeRange> deletionList = null;
    if (modsToMemtable != null) {
      deletionList =
          ModificationUtils.constructDeletionList(
              partialPath, memTable, modsToMemtable, timeLowerBound);
    }
    return new ReadOnlyMemChunk(
        context,
        partialPath.getMeasurement(),
        partialPath.getMeasurementSchema().getType(),
        partialPath.getMeasurementSchema().getEncodingType(),
        tvListQueryMap,
        partialPath.getMeasurementSchema().getProps(),
        deletionList);
  }

  @Override
  public List<IChunkMetadata> getVisibleMetadataListFromWriter(
      RestorableTsFileIOWriter writer,
      TsFileResource tsFileResource,
      QueryContext context,
      long timeLowerBound) {
    List<Modification> modifications = context.getPathModifications(tsFileResource, partialPath);

    List<IChunkMetadata> chunkMetadataList =
        new ArrayList<>(
            writer.getVisibleMetadataList(
                partialPath.getIDeviceID(),
                partialPath.getMeasurement(),
                partialPath.getSeriesType()));

    ModificationUtils.modifyChunkMetaData(chunkMetadataList, modifications);
    chunkMetadataList.removeIf(x -> x.getEndTime() < timeLowerBound);
    return chunkMetadataList;
  }
}
