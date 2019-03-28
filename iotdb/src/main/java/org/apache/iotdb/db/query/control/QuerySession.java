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

package org.apache.iotdb.db.query.control;

import java.util.concurrent.atomic.AtomicLong;

public class QuerySession {
  /**
   * Each jdbc request has an unique jod id, job id is stored in thread local variable jobIdContainer.
   */
  private ThreadLocal<Long> jobId;

  /**
   * Each unique jdbc request(query, aggregation or others job) has an unique job id. This job id
   * will always be maintained until the request is closed. In each job, the unique file will be
   * only opened once to avoid too many opened files error.
   */
  private AtomicLong jobIdGenerator = new AtomicLong();

  private QuerySession() {
    this.jobId = new ThreadLocal<Long>(){
      @Override
      protected Long initialValue() {
        super.initialValue();
        long id = jobIdGenerator.incrementAndGet();
        OpenedFilePathsManager.getInstance().addJobId(id);
        QueryTokenManager.getInstance().addJobId(id);
        return id;
      }
    };
  }

  public long getJobId() {
    return jobId.get();
  }

  public static long getCurrentThreadJobId() {
    return QuerySessionHelper.INSTANCE.jobId.get();
  }

  private static class QuerySessionHelper {

    private static final QuerySession INSTANCE = new QuerySession();

    private QuerySessionHelper() {
    }
  }

  public static QuerySession getCurrentThreadQuerySession() {
    return QuerySessionHelper.INSTANCE;
  }




}
