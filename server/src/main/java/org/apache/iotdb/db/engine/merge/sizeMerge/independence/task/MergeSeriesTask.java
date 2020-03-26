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

package org.apache.iotdb.db.engine.merge.sizeMerge.independence.task;

import static org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RegularizationMergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.NaivePathSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.independence.recover.IndependenceMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeSeriesTask {

  private static final Logger logger = LoggerFactory.getLogger(
      MergeSeriesTask.class);

  private IndependenceMergeLogger mergeLogger;

  private String taskName;
  private MergeResource resource;
  private MergeContext mergeContext;
  private long timeBlock;
  private int concurrentMergeSeriesNum;

  private List<Pair<RestorableTsFileIOWriter, TsFileResource>> newTsFilePairs;

  MergeSeriesTask(MergeContext context, String taskName, IndependenceMergeLogger mergeLogger,
      MergeResource mergeResource, int concurrentMergeSeriesNum) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.timeBlock = IoTDBDescriptor.getInstance().getConfig().getMergeFileTimeBlock();
    this.newTsFilePairs = new ArrayList<>();
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
  }

  List<TsFileResource> mergeSeries() throws IOException {

    //TODO
    return null;
  }
}
