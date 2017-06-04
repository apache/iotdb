package cn.edu.thu.tsfiledb.engine.bufferwrite;

/**
 * @author kangrong
 *
 */
@FunctionalInterface
public interface Action {
	void act() throws Exception;
}
