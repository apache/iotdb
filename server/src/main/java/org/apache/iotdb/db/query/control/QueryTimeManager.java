package org.apache.iotdb.db.query.control;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to monitor the executing time of each query.
 * </p>
 * Once one is over the threshold, it will be killed and return the time out exception.
 */
public class QueryTimeManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(QueryTimeManager.class);

  /**
   * the max executing time of query.
   */
  private int MAX_QUERY_TIME = IoTDBDescriptor.getInstance().getConfig().getQueryTimeThreshold();

  /**
   * the examine period of query time manager.
   */
  private int examinePeriod = 30000;

  /**
   * the key of queryStartTimeMap is the query id and the value of queryStartTimeMap is the start
   * time of this query.
   */
  private Map<Long, Long> queryStartTimeMap;
  /**
   * the key of queryThreadMap is the query id and the value of queryThreadMap is the executing
   * threads of this query.
   */
  private Map<Long, List<Thread>> queryThreadMap;

  private ScheduledExecutorService executorService;

  private QueryTimeManager() {
    queryStartTimeMap = new ConcurrentHashMap<>();
    queryThreadMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        "query-time-manager");

    closeOverTimeQueryInFixTime();
  }

  private void closeOverTimeQueryInFixTime() {
    executorService.scheduleAtFixedRate(() -> {
      synchronized (this) {
        long currentTime = System.currentTimeMillis();
        boolean hasOverTimeQuery = false;
        for (Entry<Long, Long> entry : queryStartTimeMap.entrySet()) {
          if (currentTime - entry.getValue() >= MAX_QUERY_TIME) {
            killQuery(entry.getKey());
            hasOverTimeQuery = true;
          }
        }
        // TODO
        if (hasOverTimeQuery) {
          try {
            throw new TimeoutException("Query is over time, please check your query statement");
          } catch (TimeoutException e) {
            logger.error("Query is over time, ");
          }
        }
      }
    }, 0, examinePeriod, TimeUnit.MILLISECONDS);
  }

  private void killQuery(long queryId) {
    List<Thread> threads = queryThreadMap.get(queryId);
    for (Thread thread : threads) {
      thread.interrupt();
    }
    unRegisterQuery(queryId);
  }

  public void registerQuery(long queryId, long startTime) {
    queryStartTimeMap.put(queryId, startTime);
  }

  public void registerQueryThread(long queryId, Thread queryThread) {
    queryThreadMap.putIfAbsent(queryId, new ArrayList<>()).add(queryThread);
  }

  public void unRegisterQuery(long queryId) {
    queryStartTimeMap.remove(queryId);
    queryThreadMap.remove(queryId);
  }

  public static QueryTimeManager getInstance() {
    return QueryTimeManagerHelper.INSTANCE;
  }

  @Override
  public void start() {
    // Do Nothing
  }

  @Override
  public void stop() {
    if (executorService == null || executorService.isShutdown()) {
      return;
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("Query time monitor service could not be shutdown.", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.QUERY_TIME_MANAGER;
  }

  private static class QueryTimeManagerHelper {

    private static final QueryTimeManager INSTANCE = new QueryTimeManager();

    private QueryTimeManagerHelper() {
    }
  }
}
