package org.apache.iotdb.db.pipe.agent.task.execution;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;

import java.util.concurrent.ExecutorService;

public class PipeExternalSourceExecutor  {

    private final ExecutorService executorService;

    public PipeExternalSourceExecutor() {
        executorService =
                IoTDBThreadPoolFactory.newFixedThreadPool(16, ThreadName.PIPE_EXTERNAL_EXTRACTOR_POOL.getName());
    }


}
