package org.apache.iotdb.metrics.micrometer;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;
import org.apache.iotdb.metrics.type.Timer;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import org.junit.BeforeClass;
import org.junit.Test;

public class MicrometerMetricManagerTest {

    @BeforeClass
    public static void init() {
        System.setProperty("line.separator", "\n");
        // set up path of yml
        System.setProperty("IOTDB_CONF", "src/test/resources");
    }

    private void getOrCreateDifferentMetricsWithSameName() {
        MetricManager metricManager = MetricService.getMetricManager();
        Timer timer = metricManager.getOrCreateTimer("metric", "tag1", "tag2");
        assertNotNull(timer);
        metricManager.getOrCreateCounter("metric", "tag1", "tag2");
    }

    @Test
    public void getOrCreateDifferentMetricsWithSameNameTest() {
        assertThrows(IllegalArgumentException.class, this::getOrCreateDifferentMetricsWithSameName);
    }
}
