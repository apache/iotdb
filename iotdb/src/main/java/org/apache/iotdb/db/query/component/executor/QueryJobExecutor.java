/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.component.executor;

import org.apache.iotdb.db.query.component.job.QueryJob;
import org.apache.iotdb.db.query.component.job.QueryJobExecutionMessage;
import org.apache.iotdb.db.query.component.job.QueryEngineImpl;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.db.query.component.job.QueryEngine;

public abstract class QueryJobExecutor implements Runnable {

    private QueryJob queryJob;
    private QueryEngine queryEngine;

    protected QueryJobExecutor(QueryJob queryJob) {
        this.queryJob = queryJob;
        this.queryEngine = QueryEngineImpl.getInstance();
    }

    public abstract QueryDataSet execute() throws InterruptedException;

    @Override
    public void run() {
        try {
            QueryDataSet queryDataSet = execute();
            queryEngine.finishJob(queryJob, queryDataSet);
        } catch (InterruptedException e) {
            queryJob.setMessage(new QueryJobExecutionMessage(e.getMessage()));
            queryEngine.terminateJob(queryJob);
        } catch (Exception e) {
            queryJob.setMessage(new QueryJobExecutionMessage("Unexpected Error:" + e.getMessage()));
            queryEngine.terminateJob(queryJob);
        }
    }
}
