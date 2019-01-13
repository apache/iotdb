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
package org.apache.iotdb.db.query.component.job;

import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public interface QueryJobFuture {

    /**
     * Wait until corresponding QueryJob is finished. This method is synchronized and invoking this method will block
     * current thread. An InterruptedException will be thrown if current thread is interrupted.
     */
    void waitToFinished() throws InterruptedException;

    /**
     * Terminate corresponding QueryJob. This method is synchronized and invoking will be blocked until corresponding
     * QueryJob is terminated. This method is synchronized and invoking this method will block current thread. An
     * InterruptedException will be thrown if current thread is interrupted.
     */
    void terminateCurrentJob() throws InterruptedException;

    /**
     * Get current status of corresponding QueryJob
     * 
     * @return status
     */
    QueryJobStatus getCurrentStatus();

    /**
     * Retrieve OnePassQueryDataSet from EngineQueryRouter result pool.
     * 
     * @return null if the queryJob is not finished.
     */
    QueryDataSet retrieveQueryDataSet();
}
