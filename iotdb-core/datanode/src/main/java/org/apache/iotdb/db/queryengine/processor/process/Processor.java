package org.apache.iotdb.db.queryengine.processor.process;

import org.apache.iotdb.db.queryengine.processor.state.ProcessState;

public interface Processor<T> {
  ProcessState<T> process();
}
