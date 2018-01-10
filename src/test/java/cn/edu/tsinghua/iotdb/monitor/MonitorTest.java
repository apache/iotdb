package cn.edu.tsinghua.iotdb.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

/**
 * @author Liliang
 */

public class MonitorTest {
    private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();

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
            assertEquals(true, mManager.pathExist(
                    MonitorConstants.statStorageGroupPrefix
                            + MonitorConstants.MONITOR_PATH_SEPERATOR
                            + MonitorConstants.fileNodeManagerPath
                            + MonitorConstants.MONITOR_PATH_SEPERATOR + statParam)
            );
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
            fManager.closeAll();
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
