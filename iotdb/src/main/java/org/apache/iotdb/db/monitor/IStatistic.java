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
package org.apache.iotdb.db.monitor;

import org.apache.iotdb.tsfile.write.record.TSRecord;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public interface IStatistic {
    /**
     * @return A HashMap that contains the module seriesPath like: root.stats.write.global, and its value is TSRecord
     *         format contains all statistics measurement
     */
    HashMap<String, TSRecord> getAllStatisticsValue();

    /**
     * A method to register statistics info
     */
    void registStatMetadata();

    /**
     * Get all module's statistics parameters as time-series seriesPath
     *
     * @return a list of string like "root.stats.xxx.statisticsParams",
     */
    List<String> getAllPathForStatistic();

    /**
     *
     * @return a HashMap contains the names and values of the statistics parameters
     */
    HashMap<String, AtomicLong> getStatParamsHashMap();
}
