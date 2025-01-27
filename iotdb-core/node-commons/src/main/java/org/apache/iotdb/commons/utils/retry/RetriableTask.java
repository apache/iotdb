package org.apache.iotdb.commons.utils.retry;

/**
* This interface defines a retriable task
* @param <T> return type of this task
*/
@FunctionalInterface
public interface RetriableTask<T> {
  T execute() throws Exception;
}