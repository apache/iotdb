package org.apache.iotdb.db.query.control;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.utils.Pair;
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
   * the examine period of query time manager, 30s
   */
  private int examinePeriod = 30000;

  /**
   * the key of queryStartTimeMap is the query id and the value of queryStartTimeMap is the start
   * time and the sql of this query.
   */
  private Map<Long, Pair<Long, String>> queryInfoMap;
  /**
   * the key of queryThreadMap is the query id and the value of queryThreadMap is the executing
   * threads of this query.
   * Only main thread is put in this map since the sub threads are maintain by the thread pool.
   * The thread allocated for readTask will change every time, so we have to access this map
   * frequently, which will lead to big performance cost.
   */
  private Map<Long, Thread> queryThreadMap;

  private ScheduledExecutorService executorService;

  private QueryTimeManager() {
    queryInfoMap = new ConcurrentHashMap<>();
    queryThreadMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        "query-time-manager");

    closeOverTimeQueryInFixTime();
  }

  private void closeOverTimeQueryInFixTime() {
    executorService.scheduleAtFixedRate(() -> {
      long currentTime = System.currentTimeMillis();
      for (Entry<Long, Pair<Long, String>> entry : queryInfoMap.entrySet()) {
        if (currentTime - entry.getValue().left >= MAX_QUERY_TIME) {
          killQuery(entry.getKey());
          logger.error("Query is time out with queryId " + entry.getKey());
        }
      }
    }, 0, examinePeriod, TimeUnit.MILLISECONDS);
  }

  public void killQuery(long queryId) {
    queryThreadMap.get(queryId).interrupt();
    unRegisterQuery(queryId);
  }

  public void registerQuery(long queryId, long startTime, String sql, Thread queryThread) {
    queryInfoMap.put(queryId, new Pair<>(startTime, sql));
    queryThreadMap.put(queryId, queryThread);
  }

  public void unRegisterQuery(long queryId) {
    queryInfoMap.remove(queryId);
    queryThreadMap.remove(queryId);
  }

  public boolean isQueryInterrupted(long queryId) {
    return queryThreadMap.get(queryId).isInterrupted();
  }

  public Map<Long, Pair<Long, String>> getQueryInfoMap() {
    return queryInfoMap;
  }

  public Map<Long, Thread> getQueryThreadMap() {
    return queryThreadMap;
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
