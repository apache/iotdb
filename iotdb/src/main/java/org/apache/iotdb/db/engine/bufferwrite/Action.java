package org.apache.iotdb.db.engine.bufferwrite;

/**
 * @author kangrong
 *
 */
@FunctionalInterface
public interface Action {
	void act() throws Exception;
}
