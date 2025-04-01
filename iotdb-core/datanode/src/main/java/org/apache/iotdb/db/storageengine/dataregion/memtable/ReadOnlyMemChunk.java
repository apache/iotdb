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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * ReadOnlyMemChunk is a snapshot of the working MemTable and flushing memtable in the memory used
 * for querying.
 */
public class ReadOnlyMemChunk {

  protected final QueryContext context;

  private String measurementUid;

  private TSDataType dataType;

  private static final Logger logger = LoggerFactory.getLogger(ReadOnlyMemChunk.class);

  protected IChunkMetadata cachedMetaData;

  private int floatPrecision;
  private TSEncoding encoding;
  private List<TimeRange> deletionList;

  // Read only chunk is now regarded as multiple pages. Apart from chunk statistics,
  // we need to collect page statistic.
  private List<Statistics<? extends Serializable>> pageStatisticsList;

  // TVList and its rowCount during query
  private Map<TVList, Integer> tvListQueryMap;

  private MemPointIterator timeValuePairIterator;

  protected final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  protected ReadOnlyMemChunk(QueryContext context) {
    this.context = context;
  }

  public ReadOnlyMemChunk(
      QueryContext context,
      String measurementUid,
      TSDataType dataType,
      TSEncoding encoding,
      Map<TVList, Integer> tvListQueryMap,
      Map<String, String> props,
      List<TimeRange> deletionList)
      throws IOException, QueryProcessException {
    this.context = context;
    this.measurementUid = measurementUid;
    this.dataType = dataType;
    int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
    if (props != null && props.containsKey(Encoder.MAX_POINT_NUMBER)) {
      try {
        floatPrecision = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
      } catch (NumberFormatException e) {
        logger.warn(
            "The format of MAX_POINT_NUMBER {}  is not correct."
                + " Using default float precision.",
            props.get(Encoder.MAX_POINT_NUMBER));
      }
      if (floatPrecision < 0) {
        logger.warn(
            "The MAX_POINT_NUMBER shouldn't be less than 0." + " Using default float precision {}.",
            TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
        floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
      }
    }
    this.floatPrecision = floatPrecision;
    this.encoding = encoding;
    this.deletionList = deletionList;
    this.tvListQueryMap = tvListQueryMap;
    this.pageStatisticsList = new ArrayList<>();
    this.context.addTVListToSet(tvListQueryMap);
  }

  public void sortTvLists() {
    for (Map.Entry<TVList, Integer> entry : getTvListQueryMap().entrySet()) {
      TVList tvList = entry.getKey();
      int queryRowCount = entry.getValue();
      if (!tvList.isSorted() && queryRowCount > tvList.seqRowCount()) {
        tvList.sort();
      }
    }
  }

  public void initChunkMetaFromTvLists() {
    // create chunk statistics
    Statistics<? extends Serializable> chunkStatistics = Statistics.getStatsByType(dataType);
    List<TVList> tvLists = new ArrayList<>(tvListQueryMap.keySet());
    timeValuePairIterator =
        MemPointIteratorFactory.create(dataType, tvLists, deletionList, floatPrecision, encoding);
    while (timeValuePairIterator.hasNextBatch()) {
      // statistics for current batch
      Statistics<? extends Serializable> pageStatistics = Statistics.getStatsByType(dataType);
      pageStatisticsList.add(pageStatistics);

      TsBlock tsBlock = timeValuePairIterator.nextBatch();
      if (!tsBlock.isEmpty()) {
        switch (dataType) {
          case BOOLEAN:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              long time = tsBlock.getTimeByIndex(i);
              chunkStatistics.update(time, tsBlock.getColumn(0).getBoolean(i));
              pageStatistics.update(time, tsBlock.getColumn(0).getBoolean(i));
            }
            break;
          case INT32:
          case DATE:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              long time = tsBlock.getTimeByIndex(i);
              chunkStatistics.update(time, tsBlock.getColumn(0).getInt(i));
              pageStatistics.update(time, tsBlock.getColumn(0).getInt(i));
            }
            break;
          case INT64:
          case TIMESTAMP:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              long time = tsBlock.getTimeByIndex(i);
              chunkStatistics.update(time, tsBlock.getColumn(0).getLong(i));
              pageStatistics.update(time, tsBlock.getColumn(0).getLong(i));
            }
            break;
          case FLOAT:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              long time = tsBlock.getTimeByIndex(i);
              chunkStatistics.update(time, tsBlock.getColumn(0).getFloat(i));
              pageStatistics.update(time, tsBlock.getColumn(0).getFloat(i));
            }
            break;
          case DOUBLE:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              long time = tsBlock.getTimeByIndex(i);
              chunkStatistics.update(time, tsBlock.getColumn(0).getDouble(i));
              pageStatistics.update(time, tsBlock.getColumn(0).getDouble(i));
            }
            break;
          case TEXT:
          case BLOB:
          case STRING:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              long time = tsBlock.getTimeByIndex(i);
              chunkStatistics.update(time, tsBlock.getColumn(0).getBinary(i));
              pageStatistics.update(time, tsBlock.getColumn(0).getBinary(i));
            }
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataType));
        }
      }
    }

    // chunk meta
    IChunkMetadata metaData =
        new ChunkMetadata(measurementUid, dataType, null, null, 0, chunkStatistics);
    metaData.setChunkLoader(new MemChunkLoader(context, this));
    metaData.setVersion(Long.MAX_VALUE);
    cachedMetaData = metaData;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean isEmpty() {
    return count() == 0;
  }

  public IChunkMetadata getChunkMetaData() {
    return cachedMetaData;
  }

  @TestOnly
  public IPointReader getPointReader() {
    for (Map.Entry<TVList, Integer> entry : tvListQueryMap.entrySet()) {
      TVList tvList = entry.getKey();
      int queryLength = entry.getValue();
      if (!tvList.isSorted() && queryLength > tvList.seqRowCount()) {
        tvList.sort();
      }
    }
    TsBlock tsBlock = buildTsBlock();
    return tsBlock.getTsBlockSingleColumnIterator();
  }

  private TsBlock buildTsBlock() {
    try {
      TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(dataType));
      writeValidValuesIntoTsBlock(builder);
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // read all data in memory chunk and write to tsblock
  private void writeValidValuesIntoTsBlock(TsBlockBuilder builder) throws IOException {
    List<TVList> tvLists = new ArrayList<>(tvListQueryMap.keySet());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(
            getDataType(), tvLists, deletionList, floatPrecision, encoding);

    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      builder.getTimeColumnBuilder().writeLong(tvPair.getTimestamp());
      switch (dataType) {
        case BOOLEAN:
          builder.getColumnBuilder(0).writeBoolean(tvPair.getValue().getBoolean());
          break;
        case INT32:
        case DATE:
          builder.getColumnBuilder(0).writeInt(tvPair.getValue().getInt());
          break;
        case INT64:
        case TIMESTAMP:
          builder.getColumnBuilder(0).writeLong(tvPair.getValue().getLong());
          break;
        case FLOAT:
          builder.getColumnBuilder(0).writeFloat(tvPair.getValue().getFloat());
          break;
        case DOUBLE:
          builder.getColumnBuilder(0).writeDouble(tvPair.getValue().getDouble());
          break;
        case TEXT:
        case STRING:
        case BLOB:
          builder.getColumnBuilder(0).writeBinary(tvPair.getValue().getBinary());
          break;
        default:
          break;
      }
      builder.declarePosition();
    }
  }

  public Map<TVList, Integer> getTvListQueryMap() {
    return tvListQueryMap;
  }

  private int count() {
    int count = 0;
    for (TVList list : tvListQueryMap.keySet()) {
      count += list.count();
    }
    return count;
  }

  public int getFloatPrecision() {
    return floatPrecision;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public List<TimeRange> getDeletionList() {
    return deletionList;
  }

  public List<Statistics<? extends Serializable>> getPageStatisticsList() {
    return pageStatisticsList;
  }

  public QueryContext getContext() {
    return context;
  }

  @TestOnly
  public TsBlock getTsBlock() {
    return null;
  }

  public MemPointIterator getMemPointIterator() {
    return timeValuePairIterator;
  }

  public int getMaxNumberOfPointsInPage() {
    return MAX_NUMBER_OF_POINTS_IN_PAGE;
  }
}
