package cn.edu.tsinghua.iotdb.engine.memcontrol;

/**
 * This class defines what act will be taken if memory reach a certain threshold.
 */
//0910: 'reach'->'reaches'
    //0910:三种策略
public interface Policy {
    void execute();
}
