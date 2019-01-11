package cn.edu.tsinghua.iotdb.query.component.job;

import cn.edu.tsinghua.iotdb.query.component.executor.QueryJobExecutor;
import cn.edu.tsinghua.iotdb.query.component.job.QueryJob;


public interface QueryJobDispatcher {

    QueryJobExecutor dispatch(QueryJob queryJob);

}
