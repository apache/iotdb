package org.apache.iotdb.db.newsync.pipedata.queue;

import org.apache.iotdb.db.newsync.pipedata.PipeData;

import java.util.List;

public interface PipeDataQueue {
    boolean offer(PipeData data);

    List<PipeData> pull(long serialNumber);

    PipeData blockingPull();

    void commit();

    void clear();
}
