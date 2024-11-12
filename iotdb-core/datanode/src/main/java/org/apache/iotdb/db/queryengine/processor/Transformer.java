package org.apache.iotdb.db.queryengine.processor;

import org.apache.iotdb.db.queryengine.processor.state.TransformationState;

public interface Transformer<T, R> {
  TransformationState<R> process(T element);
}
