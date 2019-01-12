package org.apache.iotdb.db.query.component.job;

import org.apache.iotdb.db.query.component.executor.QueryJobExecutor;
import org.apache.iotdb.db.query.component.job.QueryJob;
import org.apache.iotdb.db.query.component.executor.QueryJobExecutor;


public interface QueryJobDispatcher {

    QueryJobExecutor dispatch(QueryJob queryJob);

}
