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
package org.apache.iotdb.db.query.externalsort;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.query.externalsort.adapter.ByTimestampReaderAdapter;
import org.apache.iotdb.db.query.reader.chunk.ChunkReaderWrap;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleExternalSortEngine implements ExternalSortJobEngine {

  private ExternalSortJobScheduler scheduler;

  private String queryDir;
  private int minExternalSortSourceCount;
  private boolean enableExternalSort;
  private static final Logger logger = LoggerFactory.getLogger(SimpleExternalSortEngine.class);

  private SimpleExternalSortEngine() {
    queryDir = IoTDBDescriptor.getInstance().getConfig().getQueryDir() + File.separator;
    minExternalSortSourceCount =
        IoTDBDescriptor.getInstance().getConfig().getExternalSortThreshold();
    enableExternalSort = IoTDBDescriptor.getInstance().getConfig().isEnableExternalSort();
    scheduler = ExternalSortJobScheduler.getInstance();

    // create queryDir
    try {
      FileUtils.deleteDirectory(new File(queryDir));
      FileUtils.forceMkdir(new File(queryDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }
  }

  @Override
  public List<IPointReader> executeForIPointReader(
      long queryId, List<ChunkReaderWrap> chunkReaderWraps) throws IOException {
    if (!enableExternalSort || chunkReaderWraps.size() < minExternalSortSourceCount) {
      return generateIPointReader(chunkReaderWraps, 0, chunkReaderWraps.size());
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "query {} measurement {} uses external sort.",
          queryId,
          chunkReaderWraps.get(0).getMeasurementUid());
    }
    ExternalSortJob job = createJob(queryId, chunkReaderWraps);
    return job.executeForIPointReader();
  }

  @Override
  public List<IReaderByTimestamp> executeForByTimestampReader(
      long queryId, List<ChunkReaderWrap> chunkReaderWraps) throws IOException {
    if (!enableExternalSort || chunkReaderWraps.size() < minExternalSortSourceCount) {
      return generateIReaderByTimestamp(chunkReaderWraps, 0, chunkReaderWraps.size());
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "query {} measurement {} uses external sort.",
          queryId,
          chunkReaderWraps.get(0).getMeasurementUid());
    }
    ExternalSortJob job = createJob(queryId, chunkReaderWraps);
    return convert(job.executeForIPointReader());
  }

  @Override
  public ExternalSortJob createJob(long queryId, List<ChunkReaderWrap> readerWrapList) {
    long jobId = scheduler.genJobId();
    List<ExternalSortJobPart> ret = new ArrayList<>();
    for (ChunkReaderWrap readerWrap : readerWrapList) {
      ret.add(new SingleSourceExternalSortJobPart(readerWrap));
    }

    int partId = 0;
    while (ret.size() >= minExternalSortSourceCount) {
      List<ExternalSortJobPart> tmpPartList = new ArrayList<>();
      for (int i = 0; i < ret.size(); ) {
        int toIndex = Math.min(i + minExternalSortSourceCount, ret.size());
        List<ExternalSortJobPart> partGroup = ret.subList(i, toIndex);
        i = toIndex;
        String tmpFilePath = queryDir + jobId + "_" + partId;
        MultiSourceExternalSortJobPart part =
            new MultiSourceExternalSortJobPart(queryId, tmpFilePath, partGroup);
        tmpPartList.add(part);
        partId++;
      }
      ret = tmpPartList;
    }
    return new ExternalSortJob(ret);
  }

  String getQueryDir() {
    return queryDir;
  }

  void setQueryDir(String queryDir) {
    this.queryDir = queryDir;
  }

  int getMinExternalSortSourceCount() {
    return minExternalSortSourceCount;
  }

  void setMinExternalSortSourceCount(int minExternalSortSourceCount) {
    this.minExternalSortSourceCount = minExternalSortSourceCount;
  }

  /** init IPointReader with ChunkReaderWrap. */
  private List<IPointReader> generateIPointReader(
      List<ChunkReaderWrap> readerWraps, final int start, final int size) throws IOException {
    List<IPointReader> pointReaderList = new ArrayList<>();
    for (int i = start; i < start + size; i++) {
      pointReaderList.add(readerWraps.get(i).getIPointReader());
    }
    return pointReaderList;
  }

  /** init IReaderByTimestamp with ChunkReaderWrap. */
  private List<IReaderByTimestamp> generateIReaderByTimestamp(
      List<ChunkReaderWrap> readerWraps, final int start, final int size) throws IOException {
    List<IReaderByTimestamp> readerByTimestampList = new ArrayList<>();
    for (int i = start; i < start + size; i++) {
      readerByTimestampList.add(readerWraps.get(i).getIReaderByTimestamp());
    }
    return readerByTimestampList;
  }

  /**
   * convert IPointReader to implement interface of IReaderByTimestamp.
   *
   * @param pointReaderList reader list that implements IPointReader
   * @return reader list that implements IReaderByTimestamp
   */
  private List<IReaderByTimestamp> convert(List<IPointReader> pointReaderList) {
    List<IReaderByTimestamp> readerByTimestampList = new ArrayList<>();
    for (IPointReader pointReader : pointReaderList) {
      readerByTimestampList.add(new ByTimestampReaderAdapter(pointReader));
    }
    return readerByTimestampList;
  }

  private static class SimpleExternalSortJobEngineHelper {

    private static final SimpleExternalSortEngine INSTANCE = new SimpleExternalSortEngine();
  }

  public static SimpleExternalSortEngine getInstance() {
    return SimpleExternalSortJobEngineHelper.INSTANCE;
  }
}
