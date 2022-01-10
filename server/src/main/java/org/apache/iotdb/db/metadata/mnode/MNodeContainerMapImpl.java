package org.apache.iotdb.db.metadata.mnode;

import java.util.concurrent.ConcurrentHashMap;

public class MNodeContainerMapImpl<E extends IMNode> extends ConcurrentHashMap<String, E>
    implements IMNodeContainer<E> {}
