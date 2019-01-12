package org.apache.iotdb.db.engine.memcontrol;

/**
 * This class defines what act will be taken if memory reaches a certain threshold.
 */
public interface Policy {
    void execute();
}
