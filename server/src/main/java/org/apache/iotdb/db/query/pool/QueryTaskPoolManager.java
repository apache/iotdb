package org.apache.iotdb.db.query.pool;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.AbstractPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTaskPoolManager extends AbstractPoolManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryTaskPoolManager.class);

  private QueryTaskPoolManager() {
    int threadCnt = IoTDBDescriptor.getInstance().getConfig().getConcurrentQueryThread();
    pool = IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, ThreadName.QUERY_SERVICE.getName());
  }

  public static QueryTaskPoolManager getInstance() {
    return QueryTaskPoolManager.InstanceHolder.instance;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public String getName() {
    return "query task";
  }

  @Override
  public void start() {
    if (pool == null) {
      int threadCnt = IoTDBDescriptor.getInstance().getConfig().getConcurrentQueryThread();
      pool = IoTDBThreadPoolFactory
              .newFixedThreadPool(threadCnt, ThreadName.QUERY_SERVICE.getName());
    }

  }

  @Override
  public void stop() {
    if (pool != null) {
      close();
      pool = null;
    }
  }

  private static class InstanceHolder {

    private InstanceHolder() {
      //allowed to do nothing
    }

    private static QueryTaskPoolManager instance = new QueryTaskPoolManager();
  }
}
