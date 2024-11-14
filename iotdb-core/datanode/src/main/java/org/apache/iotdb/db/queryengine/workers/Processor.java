package org.apache.iotdb.db.queryengine.workers;

import org.apache.iotdb.db.queryengine.workers.state.ProcessState;

public interface Processor<T> {
  ProcessState<T> process();
}
