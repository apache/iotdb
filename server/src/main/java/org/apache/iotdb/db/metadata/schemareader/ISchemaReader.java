package org.apache.iotdb.db.metadata.schemareader;

import org.apache.iotdb.db.metadata.schemainfo.ISchemaInfo;

import java.util.Iterator;

public interface ISchemaReader<R extends ISchemaInfo> extends Iterator<R> {

  @Override
  boolean hasNext();

  @Override
  R next();
}
