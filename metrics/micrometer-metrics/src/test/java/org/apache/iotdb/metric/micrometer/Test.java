package org.apache.iotdb.metric.micrometer;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;

import java.util.UUID;

/**
 * @Author stormbroken
 * Create by 2021/07/13
 * @Version 1.0
 **/

public class Test {
    MetricManager metricManager = MetricService.getMetricManager();

    private static String createString(int length) {
        UUID randomUUID = UUID.randomUUID();
        return randomUUID.toString().replaceAll("-", "").substring(0, length);
    }

    private long createMeter(Integer meterNumber, Integer tagLength, Integer tagNumber){
        long start = System.currentTimeMillis();
        for(int i = 0; i < meterNumber; i ++){
            String[] t = new String[tagNumber];
            for(int j = 0; j < meterNumber; j ++){
                t[j] = createString(tagLength);
            }
            metricManager.getOrCreateCounter("counter" + i, t);
        }
        long stop = System.currentTimeMillis();
        System.out.println(stop - start);
        return stop - start;
    }

    public static void main(String[] args) {
        Test test = new Test();
        test.createMeter(1000000, 3, 10);
    }
}
