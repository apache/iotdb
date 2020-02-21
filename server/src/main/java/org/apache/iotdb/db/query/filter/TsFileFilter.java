package org.apache.iotdb.db.query.filter;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

@FunctionalInterface
public interface TsFileFilter {
  boolean fileNotSatisfy(TsFileResource resource);
}
