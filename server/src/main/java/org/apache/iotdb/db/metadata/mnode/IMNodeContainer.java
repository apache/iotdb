package org.apache.iotdb.db.metadata.mnode;

import java.util.Map;

public interface IMNodeContainer<E extends IMNode> extends Map<String, E> {}
