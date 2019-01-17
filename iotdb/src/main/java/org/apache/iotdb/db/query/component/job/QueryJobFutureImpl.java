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
package org.apache.iotdb.db.query.component.job;

import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryJobFutureImpl implements QueryJobFuture {

  private static final Logger logger = LoggerFactory.getLogger(QueryJobFutureImpl.class);
  private QueryJob queryJob;

  // private QueryEngine queryEngine;

  public QueryJobFutureImpl(QueryJob queryJob) {
    this.queryJob = queryJob;
    // this.queryEngine = QueryEngineImpl.getInstance();
  }

  @Override
  public void waitToFinished() throws InterruptedException {
    synchronized (queryJob) {
      if (queryJobIsDone(queryJob)) {
        return;
      } else {
        queryJob.wait();
      }
    }
  }

  @Override
  public void terminateCurrentJob() throws InterruptedException {
    synchronized (queryJob) {
      if (!queryJobIsDone(queryJob)) {
        queryJob.setStatus(QueryJobStatus.WAITING_TO_BE_TERMINATED);
        queryJob.wait();
      }
    }
  }

  @Override
  public QueryJobStatus getCurrentStatus() {
    return queryJob.getStatus();
  }

  @Override
  public QueryDataSet retrieveQueryDataSet() {
    return null;
    // return queryEngine.retrieveQueryDataSet(queryJob);
  }

  private boolean queryJobIsDone(QueryJob queryJob) {
    if (queryJob.getStatus() == QueryJobStatus.FINISHED
        || queryJob.getStatus() == QueryJobStatus.TERMINATED) {
      return true;
    }
    return false;
  }
}
