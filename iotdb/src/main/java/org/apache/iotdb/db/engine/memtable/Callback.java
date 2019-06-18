package org.apache.iotdb.db.engine.memtable;

public interface Callback {

  void call(Object... object);

}
