package org.apache.iotdb.db.metadata.mnode;

public class MNodeContainers {

  @SuppressWarnings("rawtypes")
  private static final IMNodeContainer EMPTY_CONTAINER = new MNodeContainerMapImpl();

  @SuppressWarnings("unchecked")
  public static <E extends IMNode> IMNodeContainer<E> emptyMNodeContainer() {
    return (IMNodeContainer<E>) EMPTY_CONTAINER;
  }

  public static <E extends IMNode> IMNodeContainer<E> getNewMNodeContainer() {
    return new MNodeContainerMapImpl<>();
  }
}
