package org.apache.iotdb.commons.service.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GcTimeAlerter implements JvmGcMetrics.GcTimeAlertHandler {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(GcTimeAlerter.class);

    /**
     * Alert handler func
     * User can tailor their handle logic here
     * @param gcData
     */
    @Override
    public void alert(JvmGcMetrics.GcData gcData) {
        logger.warn("Error metrics taken time: " + sdf.format(new Date(Long.parseLong(String.valueOf(gcData.getTimestamp())))));
        logger.warn("Time since monitor has been started: " + gcData.getGcMonitorRunTime() + " ms");
        logger.warn("Gc Time Percentage: " + gcData.getGcTimePercentage() + "%");
        logger.warn("Accumulated GC time: " + gcData.getAccumulatedGcTime() + " ms");
        logger.warn("Accumulated GC count: " + gcData.getAccumulatedGcCount());
    }
}
