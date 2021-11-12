package org.apache.iotdb.db.service.basic.count;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryFrequency {

  // Record query count
  private static final Logger QUERY_FREQUENCY_LOGGER = LoggerFactory.getLogger("QUERY_FREQUENCY");
  private static final AtomicInteger queryCount = new AtomicInteger(0);

  public QueryFrequency(IoTDBConfig config) {
    ScheduledExecutorService timedQuerySqlCountThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("timedQuerySqlCount");
    timedQuerySqlCountThread.scheduleAtFixedRate(
        () -> {
          if (queryCount.get() != 0) {
            QUERY_FREQUENCY_LOGGER.info(
                "Query count in current 1 minute {} ", queryCount.getAndSet(0));
          }
        },
        config.getFrequencyIntervalInMinute(),
        config.getFrequencyIntervalInMinute(),
        TimeUnit.MINUTES);
  }

  public void incrementAndGet() {
    queryCount.incrementAndGet();
  }
}
