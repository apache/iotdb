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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
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
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
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
import java.util.Set;

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

  static Map<TSDataType, Integer> countDataTypes(List<TSDataType> dataTypes) {
    Map<TSDataType, Integer> dataTypeCounts = new LinkedHashMap<>();
    for (TSDataType dataType : dataTypes) {
      dataTypeCounts.merge(dataType, 1, Integer::sum);
    }
    return dataTypeCounts;
  }

  public abstract ITimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<IChunkMetadata> chunkMetadataList,
      Filter globalTimeFilter)
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
      Filter globalTimeFilter,
      List<Integer> columnIndexList) {
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
        // columnIndexList is to track column-level access for AlignedTVList.
        // For TVList (primitive time series), it remains null and column tracking is not needed.
        if (columnIndexList != null && context instanceof FragmentInstanceContext) {
          ((FragmentInstanceContext) context).putAccessedColumns(tvList, columnIndexList);
        }
      } finally {
        tvList.unlockQueryList();
      }
    }

    TVList.RamInfo listRamInfo = null;

    // calculateRamSize (synchronized method on TVList) was previously called before
    // lockQueryList to avoid deadlock concerns. For partial clone of AlignedTVList, however
    // calculateRamSize must now be called inside the lockQueryList section because it depends on
    // accessing columns on the AlignedTVList.
    // This is safe because the lock ordering — queryListLock must always be acquired before the
    // TVList intrinsic lock (via synchronized methods like calculateRamSize, clone). So no AB-BA
    // deadlock is possible.
    while (true) {
      // The working TVList may be replaced by a concurrent query via clone-and-swap
      // (memChunk.setWorkingTVList(clone)). A queryListLock held on a detached candidate does
      // not protect the current working TVList, so after acquiring the lock, re-verify it is
      // still the current working list under the memChunk lock. If it was replaced while
      // waiting for candidate's queryListLock, retry with the current one.
      final TVList candidate = memChunk.getWorkingTVList();
      candidate.lockQueryList();
      try {
        synchronized (memChunk) {
          if (memChunk.getWorkingTVList() != candidate) {
            continue;
          }
        }

        if (copyTimeFilter != null
            && !copyTimeFilter.satisfyStartEndTime(
                candidate.getMinTime(), candidate.getMaxTime())) {
          return tvListQueryMap;
        }

        if (!isWorkMemTable) {
          /*
           * 1. Q1 queries this TVList while it is still in the working memtable and records a smaller
           *    visible row count.
           * 2. Later writes append out-of-order rows to the same TVList, then FLUSH moves the
           *    memtable to the flushing list.
           * 3. Q2 queries the flushing memtable. If Q2 directly reuses the original mutable TVList,
           *    Q2's query-side sort may reorder the indices in place.
           * 4. Q1 continues to read with its old row count and the reordered indices. The converted
           *    value index can exceed Q1's bitmap range and cause out-of-bound access.
           *
           * Therefore, this flushing branch can reuse the original list only when it is already
           * sorted or no active query is using it. Otherwise, Q2 should read from
           * workingListForFlush.
           */
          boolean canUseListDirectly =
              candidate.isSorted() || candidate.getQueryContextSet().isEmpty();
          LOGGER.debug(
              "Flushing MemTable - add current query context to mutable TVList's query list");
          if (canUseListDirectly) {
            candidate.getQueryContextSet().add(context);
            tvListQueryMap.put(candidate, candidate.rowCount());
          } else {
            TVList workingListForFlushSort =
                memChunk.initWorkingListForFlushIfNecessary(candidate, true);
            /*
             * The query will read from workingListForFlushSort, but cloneForFlushSort() only clones
             * times and indices. The value arrays and bitmaps are still shared with the original
             * list.
             *
             * Therefore, this query must also hold the original list until it finishes. Adding
             * context to list.getQueryContextSet() lets flush/query cleanup see that the original
             * list is still in use. Adding list to context.tvListSet makes
             * releaseTVListOwnedByQuery() remove this context from the original list later.
             *
             * Do not put the original list into tvListQueryMap here. The actual read path must use
             * workingListForFlushSort to avoid sorting the original list in place.
             */
            candidate.getQueryContextSet().add(context);
            context.addTVListToSet(Collections.singleton(candidate));
            // Query preparation is serialized by candidate's query-list lock, but cleanup removes
            // the context under workingListForFlushSort's own lock. Use the same lock for this add
            // to avoid concurrently mutating its HashSet. The lock order here is candidate first,
            // then workingListForFlushSort; cleanup never holds both locks at the same time.
            workingListForFlushSort.lockQueryList();
            try {
              workingListForFlushSort.getQueryContextSet().add(context);
            } finally {
              workingListForFlushSort.unlockQueryList();
            }
            tvListQueryMap.put(workingListForFlushSort, workingListForFlushSort.rowCount());
          }

          // columnIndexList is to track column-level access for AlignedTVList.
          // For TVList (primitive time series), it remains null and column tracking is not needed.
          if (columnIndexList != null && context instanceof FragmentInstanceContext) {
            ((FragmentInstanceContext) context).putAccessedColumns(candidate, columnIndexList);
          }
          return tvListQueryMap;
        }

        if (candidate.isSorted() || candidate.getQueryContextSet().isEmpty()) {
          LOGGER.debug(
              "Working MemTable - add current query context to mutable TVList's query list when it's sorted or no other query on it");
          candidate.getQueryContextSet().add(context);
          tvListQueryMap.put(candidate, candidate.rowCount());

          // columnIndexList is to track column-level access for AlignedTVList.
          // For TVList (primitive time series), it remains null and column tracking is not needed.
          if (columnIndexList != null && context instanceof FragmentInstanceContext) {
            ((FragmentInstanceContext) context).putAccessedColumns(candidate, columnIndexList);
          }
          return tvListQueryMap;
        }

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

        synchronized (memChunk) {
          // Re-check defensively before cloning and publishing the replacement. The clone and the
          // working-list swap must be done in the same memChunk critical section, so a concurrent
          // query can never observe a working TVList whose columns have already been moved away.
          if (memChunk.getWorkingTVList() != candidate) {
            continue;
          }

          // calculateRamSize (synchronized method on TVList) was previously called before
          // lockQueryList to avoid deadlock concerns. For partial clone of AlignedTVList, however
          // calculateRamSize must now be called inside the lockQueryList section because it depends
          // on accessing columns on the AlignedTVList.
          // This is safe because the lock ordering - queryListLock must always be acquired before
          // the TVList intrinsic lock (via synchronized methods like calculateRamSize, clone). So
          // no AB-BA deadlock is possible.
          Set<Integer> columnsToClone = candidate.getAccessedColumnsForQuery();
          listRamInfo =
              (columnsToClone == null)
                  ? candidate.calculateRamSize()
                  : ((AlignedTVList) candidate).calculateRamSize(columnsToClone);

          QueryContext firstQuery = candidate.getQueryContextSet().iterator().next();
          TVList cloneList = null;
          AlignedTVList.PartialClonePlan partialClonePlan = null;
          FragmentInstanceContext cloneContext =
              columnIndexList != null && context instanceof FragmentInstanceContext
                  ? (FragmentInstanceContext) context
                  : null;
          MemoryReservationManager memoryReservationManager =
              firstQuery instanceof FragmentInstanceContext
                  ? ((FragmentInstanceContext) firstQuery).getMemoryReservationContext()
                  : null;
          boolean reservationNeedsRollback = false;
          boolean replacementPublished = false;
          try {
            // Reserve before allocating the clone, so this transient memory increase is still
            // protected by query-memory admission control. Ownership is not published yet, and a
            // later preparation failure rolls this exact reservation back immediately.
            if (memoryReservationManager != null) {
              memoryReservationManager.reserveMemoryCumulatively(listRamInfo.getRamSize());
              reservationNeedsRollback = true;
            }

            // Clone and validate without changing the source list. PartialClonePlan.commit is the
            // only destructive step and is allocation-free.
            if (columnsToClone == null) {
              cloneList = candidate.clone();
            } else {
              partialClonePlan = ((AlignedTVList) candidate).preparePartialClone(columnsToClone);
              cloneList = partialClonePlan.getCloneList();
            }

            cloneList.getQueryContextSet().add(context);
            tvListQueryMap.put(cloneList, cloneList.rowCount());
            if (cloneContext != null) {
              cloneContext.putAccessedColumns(cloneList, columnIndexList);
            }

            if (partialClonePlan != null) {
              partialClonePlan.commit();
            }
            memChunk.setWorkingTVList(cloneList);
            replacementPublished = true;

            // Publish query ownership only after the replacement is fully committed. The
            // candidate query-list lock prevents its owner from being released concurrently.
            if (memoryReservationManager != null) {
              candidate.setReservedMemoryBytes(listRamInfo.getRamSize());
            }
            candidate.setOwnerQuery(firstQuery);
            reservationNeedsRollback = false;
            return tvListQueryMap;
          } catch (RuntimeException | Error failure) {
            if (reservationNeedsRollback) {
              try {
                memoryReservationManager.releaseMemoryImmediately(listRamInfo.getRamSize());
              } catch (RuntimeException | Error rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
              }
            }

            // Before commit, remove the only external reference installed for the unpublished
            // clone. Its arrays can then be reclaimed while candidate remains the working list.
            if (!replacementPublished && cloneList != null) {
              cloneList.getQueryContextSet().remove(context);
              tvListQueryMap.remove(cloneList);
              if (cloneContext != null) {
                cloneContext.removeAccessedColumns(cloneList);
              }
            }
            throw failure;
          }
        }
      } catch (MemoryNotEnoughException ex) {
        if (listRamInfo != null) {
          LOGGER.warn(
              "Failed to reserve memory for TVList: ramSize {}, timestampsSize {}, arrayMemCost {}, rowCount {}, dataTypeCounts {}",
              listRamInfo.getRamSize(),
              listRamInfo.getTimestampsSize(),
              listRamInfo.getArrayMemCost(),
              listRamInfo.getRowCount(),
              countDataTypes(listRamInfo.getDataTypes()));
        }
        throw ex;
      } finally {
        candidate.unlockQueryList();
      }
    }
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
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<IChunkMetadata> chunkMetadataList,
      Filter globalTimeFilter) {
    TimeseriesMetadata timeTimeSeriesMetadata = new TimeseriesMetadata();
    timeTimeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);
    timeTimeSeriesMetadata.setMeasurementId("");
    timeTimeSeriesMetadata.setTsDataType(TSDataType.VECTOR);

    boolean useFakeStatistics =
        !readOnlyMemChunk.isEmpty()
            && IoTDBDescriptor.getInstance().getConfig().isStreamingQueryMemChunk();
    long startTime = Long.MAX_VALUE;
    long endTime = Long.MIN_VALUE;
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
      if (!useFakeStatistics) {
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
        continue;
      }
      startTime = Math.min(startTime, chunkMetadata.getStartTime());
      endTime = Math.max(endTime, chunkMetadata.getEndTime());
    }

    for (ReadOnlyMemChunk memChunk : readOnlyMemChunk) {
      if (!memChunk.isEmpty()) {
        memChunk.sortTvLists();
        if (useFakeStatistics) {
          memChunk.initChunkMetaFromTVListsWithFakeStatistics();
          startTime = Math.min(startTime, memChunk.getChunkMetaData().getStartTime());
          endTime = Math.max(endTime, memChunk.getChunkMetaData().getEndTime());
        } else {
          memChunk.initChunkMetaFromTvLists(globalTimeFilter);
        }
        AlignedChunkMetadata alignedChunkMetadata =
            (AlignedChunkMetadata) memChunk.getChunkMetaData();
        if (!useFakeStatistics) {
          timeStatistics.mergeStatistics(
              alignedChunkMetadata.getTimeChunkMetadata().getStatistics());
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
    }

    timeTimeSeriesMetadata.setStatistics(timeStatistics);
    if (useFakeStatistics) {
      timeStatistics.setStartTime(startTime);
      timeStatistics.setEndTime(endTime);
      timeStatistics.setCount(1);
    }
    timeTimeSeriesMetadata.setModified(useFakeStatistics || modified);

    for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
      if (useFakeStatistics) {
        TimeseriesMetadata valueTimeseriesMetadata = valueTimeSeriesMetadataList.get(i);
        valueTimeseriesMetadata.getStatistics().setStartTime(startTime);
        valueTimeseriesMetadata.getStatistics().setEndTime(endTime);
        valueTimeseriesMetadata.getStatistics().setCount(1);
        valueTimeseriesMetadata.setModified(useFakeStatistics || modified);
      } else if (!exist[i]) {
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

    // column index list for the query
    List<Integer> columnIndexList =
        alignedMemChunk.buildColumnIndexList(partialPath.getSchemaList());

    // prepare AlignedTVList for query. It should clone TVList if necessary.
    Map<TVList, Integer> alignedTvListQueryMap =
        prepareTvListMapForQuery(
            context, alignedMemChunk, modsToMemtable == null, globalTimeFilter, columnIndexList);

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
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<IChunkMetadata> chunkMetadataList,
      Filter globalTimeFilter) {
    boolean useFakeStatistics =
        !readOnlyMemChunk.isEmpty()
            && IoTDBDescriptor.getInstance().getConfig().isStreamingQueryMemChunk();
    TimeseriesMetadata timeSeriesMetadata = new TimeseriesMetadata();
    timeSeriesMetadata.setMeasurementId(partialPath.getMeasurementSchema().getMeasurementId());
    timeSeriesMetadata.setTsDataType(partialPath.getMeasurementSchema().getType());
    timeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);

    long startTime = Long.MAX_VALUE;
    long endTime = Long.MIN_VALUE;
    Statistics<? extends Serializable> seriesStatistics =
        Statistics.getStatsByType(timeSeriesMetadata.getTsDataType());
    // flush chunkMetadataList one by one
    boolean isModified = false;
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      isModified = (isModified || chunkMetadata.isModified());
      if (!useFakeStatistics) {
        seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
        continue;
      }
      startTime = Math.min(startTime, chunkMetadata.getStartTime());
      endTime = Math.max(endTime, chunkMetadata.getEndTime());
    }

    for (ReadOnlyMemChunk memChunk : readOnlyMemChunk) {
      if (!memChunk.isEmpty()) {
        memChunk.sortTvLists();
        if (useFakeStatistics) {
          memChunk.initChunkMetaFromTVListsWithFakeStatistics();
          startTime = Math.min(startTime, memChunk.getChunkMetaData().getStartTime());
          endTime = Math.max(endTime, memChunk.getChunkMetaData().getEndTime());
        } else {
          memChunk.initChunkMetaFromTvLists(globalTimeFilter);
          seriesStatistics.mergeStatistics(memChunk.getChunkMetaData().getStatistics());
        }
      }
    }
    if (useFakeStatistics) {
      seriesStatistics.setStartTime(startTime);
      seriesStatistics.setEndTime(endTime);
    }
    timeSeriesMetadata.setStatistics(seriesStatistics);
    timeSeriesMetadata.setModified(useFakeStatistics || isModified);
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
        prepareTvListMapForQuery(context, memChunk, modsToMemtable == null, globalTimeFilter, null);
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
