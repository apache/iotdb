package cn.edu.tsinghua.iotdb.engine.bufferwrite;

/**
 * @author kangrong
 *
 */
@FunctionalInterface
public interface Action {
	void act() throws Exception;
}
