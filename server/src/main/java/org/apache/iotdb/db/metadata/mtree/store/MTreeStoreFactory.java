package org.apache.iotdb.db.metadata.mtree.store;

public class MTreeStoreFactory {

  private static IMTreeStore store;

  public static IMTreeStore getMTreeStore() {
    if (store == null) {
      store = new MemMTreeStore();
    }

    return store;
  }

  private MTreeStoreFactory() {}
}
