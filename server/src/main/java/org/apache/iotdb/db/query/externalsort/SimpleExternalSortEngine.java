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
 import org.apache.iotdb.db.query.reader.IPointReader;
 import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;


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
       FileUtils.forceMkdir(new File(queryDir));
     } catch (IOException e) {
       throw new StorageEngineFailureException("create system directory failed!");
     }
   }

   // This class is used in test.
   public SimpleExternalSortEngine(String queryDir, int minExternalSortSourceCount) {
     this.queryDir = queryDir;
     this.minExternalSortSourceCount = minExternalSortSourceCount;
     scheduler = ExternalSortJobScheduler.getInstance();
   }

   @Override
   public List<IPointReader> executeWithGlobalTimeFilter(long queryId, List<IPointReader> readers,
       int startPriority)
       throws IOException {
     if (readers.size() < minExternalSortSourceCount) {
       return readers;
     }
     ExternalSortJob job = createJob(queryId, readers, startPriority);
     return job.executeWithGlobalTimeFilter();
   }

   //TODO: this method could be optimized to have a better performance
   @Override
   public ExternalSortJob createJob(long queryId, List<IPointReader> readers, int startPriority)
       throws IOException {
     long jodId = scheduler.genJobId();
     List<ExternalSortJobPart> ret = new ArrayList<>();
     List<ExternalSortJobPart> tmpPartList = new ArrayList<>();
     for (IPointReader reader : readers) {
       ret.add(
           new SingleSourceExternalSortJobPart(new PriorityMergeReader(reader, startPriority++)));
     }

     int partId = 0;
     while (ret.size() >= minExternalSortSourceCount) {
       for (int i = 0; i < ret.size(); ) {
         List<ExternalSortJobPart> partGroup = new ArrayList<>();
         for (int j = 0; j < minExternalSortSourceCount && i < ret.size(); j++) {
           partGroup.add(ret.get(i));
           i++;
         }
         StringBuilder tmpFilePath = new StringBuilder(queryDir).append(jodId).append("_")
             .append(partId);
         MultiSourceExternalSortJobPart part = new MultiSourceExternalSortJobPart(queryId,
             tmpFilePath.toString(), partGroup);
         tmpPartList.add(part);
         partId++;
       }
       ret = tmpPartList;
       tmpPartList = new ArrayList<>();
     }
     return new ExternalSortJob(jodId, ret);
   }

   private static class SimpleExternalSortJobEngineHelper {

     private static SimpleExternalSortEngine INSTANCE = new SimpleExternalSortEngine();
   }

   public static SimpleExternalSortEngine getInstance() {
     return SimpleExternalSortJobEngineHelper.INSTANCE;
   }
 }
