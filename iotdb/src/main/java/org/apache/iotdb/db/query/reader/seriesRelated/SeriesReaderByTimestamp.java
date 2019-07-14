/**
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
package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceReaderByTimestamp;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceReaderByTimestamp;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * To read series data by timestamp, this class extends {@link PriorityMergeReaderByTimestamp} to
 * implement <code>IReaderByTimestamp</code> for the data.
 * <p>
 * Note that series data consists of sequence and unsequence TsFile resources.
 * <p>
 * This class is used in conjunction with {@link org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator}.
 */
public class SeriesReaderByTimestamp extends PriorityMergeReaderByTimestamp {

  public SeriesReaderByTimestamp(Path seriesPath, QueryContext context)
      throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context);

    // reader for sequence resources
    SeqResourceReaderByTimestamp seqResourceReaderByTimestamp = new SeqResourceReaderByTimestamp(
        seriesPath, queryDataSource.getSeqResources(), context);

    // reader for unsequence resources
    UnseqResourceReaderByTimestamp unseqResourceReaderByTimestamp = new UnseqResourceReaderByTimestamp(
        seriesPath, queryDataSource.getUnseqResources(), context);

    addReaderWithPriority(seqResourceReaderByTimestamp, 1);
    addReaderWithPriority(unseqResourceReaderByTimestamp, 2);
  }
}
