package org.apache.iotdb.db.engine.memcontrol;

public interface MemUser {

  /**
   * register this user in MemController.
   */
  default void register() {
    BasicMemController.getInstance().register(this);
  }

  /**
   * unregister this user in MemController.
   */
  default void unregister() {
    BasicMemController.getInstance().unregister(this);
  }
}
