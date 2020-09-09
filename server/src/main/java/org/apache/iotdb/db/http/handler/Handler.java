package org.apache.iotdb.db.http.handler;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Handler {
  protected static final Logger logger = LoggerFactory.getLogger(Handler.class);
  protected static String username;
  protected IPlanExecutor executor;
  protected Planner processor;

  Handler() {
    try {
      processor = new Planner();
      executor = new PlanExecutor();
    } catch (QueryProcessException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Check whether current user has logged in.
   *
   */
  void checkLogin() throws AuthException{
    if(username == null) {
      throw new AuthException("didn't log in iotdb");
    }
  }

  public String getUsername() {
    return username;
  }
}
