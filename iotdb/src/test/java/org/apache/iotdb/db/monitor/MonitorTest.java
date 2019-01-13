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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.metadata.MManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.TSRecord;

/**
 * @author Liliang
 */

public class MonitorTest {
    private IoTDBConfig tsdbconfig = IoTDBDescriptor.getInstance().getConfig();

    private FileNodeManager fManager = null;
    private StatMonitor statMonitor;

    @Before
    public void setUp() throws Exception {
        // origin value
        // modify stat parameter
        EnvironmentUtils.closeMemControl();
        EnvironmentUtils.envSetUp();
        tsdbconfig.enableStatMonitor = true;
        tsdbconfig.backLoopPeriodSec = 1;
    }

    @After
    public void tearDown() throws Exception {
        tsdbconfig.enableStatMonitor = false;
        statMonitor.close();
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testFileNodeManagerMonitorAndAddMetadata() {
        fManager = FileNodeManager.getInstance();
        statMonitor = StatMonitor.getInstance();
        statMonitor.registStatStorageGroup();
        fManager.getStatParamsHashMap().forEach((key, value) -> value.set(0));
        statMonitor.clearIStatisticMap();
        statMonitor.registStatistics(fManager.getClass().getSimpleName(), fManager);
        // add metadata
        MManager mManager = MManager.getInstance();
        fManager.registStatMetadata();
        HashMap<String, AtomicLong> statParamsHashMap = fManager.getStatParamsHashMap();
        for (String statParam : statParamsHashMap.keySet()) {
            assertEquals(true,
                    mManager.pathExist(MonitorConstants.statStorageGroupPrefix + MonitorConstants.MONITOR_PATH_SEPERATOR
                            + MonitorConstants.fileNodeManagerPath + MonitorConstants.MONITOR_PATH_SEPERATOR
                            + statParam));
        }
        statMonitor.activate();
        // wait for time second
        try {
            Thread.sleep(5000);
            statMonitor.close();
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Get stat data and test right

        HashMap<String, TSRecord> statHashMap = fManager.getAllStatisticsValue();

        String path = fManager.getAllPathForStatistic().get(0);
        int pos = path.lastIndexOf('.');
        TSRecord fTSRecord = statHashMap.get(path.substring(0, pos));

        assertNotEquals(null, fTSRecord);
        for (DataPoint dataPoint : fTSRecord.dataPointList) {
            String m = dataPoint.getMeasurementId();
            Long v = (Long) dataPoint.getValue();

            if (m.equals("TOTAL_REQ_SUCCESS")) {
                assertEquals(v, new Long(0));
            }
            if (m.contains("FAIL")) {
                assertEquals(v, new Long(0));
            } else if (m.contains("POINTS")) {
                assertEquals(v, new Long(0));
            } else {
                assertEquals(v, new Long(0));
            }
        }

        try {
            fManager.deleteAll();
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
