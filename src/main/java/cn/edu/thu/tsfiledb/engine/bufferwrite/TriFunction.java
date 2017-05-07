package cn.edu.thu.tsfiledb.engine.bufferwrite;

import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;

/**
 * @author kangrong
 *
 * @param <U>
 * @param <T>
 * @param <C>
 * @param <R>
 */
@FunctionalInterface
public interface TriFunction<U, T, C, R> {
	R apply(U u, T t, C c) throws WriteProcessException;
}
