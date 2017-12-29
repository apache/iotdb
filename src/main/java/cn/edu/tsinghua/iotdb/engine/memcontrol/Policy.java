package cn.edu.tsinghua.iotdb.engine.memcontrol;

/**
 * This class defines what act will be taken if memory reach a certain threshold.
 */
public interface Policy {
    void execute();
}
