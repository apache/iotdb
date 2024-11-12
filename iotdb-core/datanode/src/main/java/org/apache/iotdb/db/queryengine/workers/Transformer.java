package org.apache.iotdb.db.queryengine.workers;

import org.apache.iotdb.db.queryengine.workers.state.TransformationState;

public interface Transformer<T, R> {
  TransformationState<R> process(T element);
}
