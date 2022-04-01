package org.apache.iotdb.db.metrics;

import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MonitorType;
import org.apache.iotdb.metrics.utils.PredefinedMetric;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author Erickin
 * @create 2022-03-30-下午 3:45
 */
public class PrometheusRunTest {

  public static void main(String[] args) {
    runTest();
  }

  public static void runTest() {
    MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
    MetricsService metricsService = MetricsService.getInstance();

    metricConfig.setMonitorType(MonitorType.MICROMETER);

    ArrayList<PredefinedMetric> predefinedMetrics = new ArrayList<>();
    predefinedMetrics.add(PredefinedMetric.JVM);
    metricConfig.setPredefinedMetrics(predefinedMetrics);
    metricConfig.setEnableMetric(true);

    metricsService.startService();

    Counter count =
        metricsService
            .getMetricManager()
            .getOrCreateCounter(
                "CISDI_TEST_TEST_TEST", MetricLevel.IMPORTANT, "TestKey", "TestValue");
    System.out.println("当前指标库" + metricConfig.getMonitorType());
    while (true) {
      count.inc(100);
      //            System.out.println("测试运行中..."+System.currentTimeMillis());
      try {
        TimeUnit.SECONDS.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
