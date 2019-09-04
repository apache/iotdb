 /**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package org.apache.iotdb.db.query.externalsort;

 import java.io.File;
 import java.io.IOException;
 import java.util.ArrayList;
 import java.util.List;
 import org.apache.commons.io.FileUtils;
 import org.apache.iotdb.db.conf.IoTDBDescriptor;
 import org.apache.iotdb.db.exception.StorageEngineFailureException;
 import org.apache.iotdb.db.query.externalsort.adapter.ByTimestampReaderAdapter;
 import org.apache.iotdb.db.query.reader.IPointReader;
 import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
 import org.apache.iotdb.db.query.reader.chunkRelated.ChunkReaderWrap;


 public class SimpleExternalSortEngine implements ExternalSortJobEngine {

   private ExternalSortJobScheduler scheduler;

   private String queryDir;
   private int minExternalSortSourceCount;

   private SimpleExternalSortEngine() {
     queryDir = IoTDBDescriptor.getInstance().getConfig().getQueryDir();
     minExternalSortSourceCount = IoTDBDescriptor.getInstance().getConfig()
         .getExternalSortThreshold();
     scheduler = ExternalSortJobScheduler.getInstance();

     // create queryDir
     try {
       FileUtils.deleteDirectory(new File(queryDir));
       FileUtils.forceMkdir(new File(queryDir));
     } catch (IOException e) {
       throw new StorageEngineFailureException("create system directory failed! " + e.toString());
     }
   }

   // This constructor is used in test.
   public SimpleExternalSortEngine(String queryDir, int minExternalSortSourceCount) {
     this.queryDir = queryDir;
     this.minExternalSortSourceCount = minExternalSortSourceCount;
     scheduler = ExternalSortJobScheduler.getInstance();
   }

   @Override
   public List<IPointReader> executeForIPointReader(long queryId,
       List<ChunkReaderWrap> chunkReaderWraps)
       throws IOException {
     if (chunkReaderWraps.size() < minExternalSortSourceCount) {
       return generateIPointReader(chunkReaderWraps, 0, chunkReaderWraps.size());
     }
     ExternalSortJob job = createJob(queryId, chunkReaderWraps);
     return job.executeWithGlobalTimeFilter();
   }

   @Override
   public List<IReaderByTimestamp> executeForByTimestampReader(long queryId,
       List<ChunkReaderWrap> chunkReaderWraps) throws IOException {
     if (chunkReaderWraps.size() < minExternalSortSourceCount) {
       return generateIReaderByTimestamp(chunkReaderWraps, 0, chunkReaderWraps.size());
     }
     ExternalSortJob job = createJob(queryId, chunkReaderWraps);
     return convert(job.executeWithGlobalTimeFilter());
   }

   //TODO: this method could be optimized to have a better performance
   @Override
   public ExternalSortJob createJob(long queryId, List<ChunkReaderWrap> readerWrapList) {
     long jodId = scheduler.genJobId();
     List<ExternalSortJobPart> ret = new ArrayList<>();
     for (ChunkReaderWrap readerWrap : readerWrapList) {
       ret.add(new SingleSourceExternalSortJobPart(readerWrap));
     }

     int partId = 0;
     while (ret.size() >= minExternalSortSourceCount) {
       List<ExternalSortJobPart> tmpPartList = new ArrayList<>();
       for (int i = 0; i < ret.size(); ) {
         int toIndex = Math.min(i+minExternalSortSourceCount, ret.size());
         List<ExternalSortJobPart> partGroup = ret.subList(i, toIndex);
         i = toIndex;
         StringBuilder tmpFilePath = new StringBuilder(queryDir).append(jodId).append("_")
             .append(partId);
         MultiSourceExternalSortJobPart part = new MultiSourceExternalSortJobPart(queryId,
             tmpFilePath.toString(), partGroup);
         tmpPartList.add(part);
         partId++;
       }
       ret = tmpPartList;
     }
     return new ExternalSortJob(jodId, ret);
   }

   /**
    * init IPointReader with ChunkReaderWrap.
    */
   private List<IPointReader> generateIPointReader(List<ChunkReaderWrap> readerWraps,
       final int start, final int size) throws IOException {
     List<IPointReader> pointReaderList = new ArrayList<>();
     for (int i = start; i < start + size; i++) {
       pointReaderList.add(readerWraps.get(i).getIPointReader());
     }
     return pointReaderList;
   }

   /**
    * init IReaderByTimestamp with ChunkReaderWrap.
    */
   private List<IReaderByTimestamp> generateIReaderByTimestamp(List<ChunkReaderWrap> readerWraps,
       final int start, final int size) throws IOException {
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

     private static SimpleExternalSortEngine INSTANCE = new SimpleExternalSortEngine();
   }

   public static SimpleExternalSortEngine getInstance() {
     return SimpleExternalSortJobEngineHelper.INSTANCE;
   }
 }
