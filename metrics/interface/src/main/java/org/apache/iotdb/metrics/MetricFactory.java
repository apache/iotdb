package org.apache.iotdb.metrics;

import java.util.Map;

public interface MetricFactory {

   /**
    *
    * repeated calling the method will return the same Object instance.
    *
    * @param namespace
    * @return
    */
   MetricRegistry getMetric(String namespace);

   void enableKnownMetric(KnownMetric metric);
   Map<String, MetricRegistry> getAllMetrics();
   boolean isEnable();

}
