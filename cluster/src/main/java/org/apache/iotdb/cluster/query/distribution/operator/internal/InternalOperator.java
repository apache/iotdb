package org.apache.iotdb.cluster.query.distribution.operator.internal;

import org.apache.iotdb.cluster.query.distribution.operator.ExecutableOperator;

// 从 buffer 拉数据
// 推送到下游的逻辑
public abstract class InternalOperator<T> extends ExecutableOperator<T> {}
