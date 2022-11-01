package org.apache.iotdb.db.metadata.schemareader;

import org.apache.iotdb.db.metadata.schemainfo.ISchemaInfo;

import java.util.Iterator;

public class SchemaReaderFakeImpl<R extends ISchemaInfo> implements ISchemaReader<R> {
  private final Iterator<R> schemaInfoIterator;

  public SchemaReaderFakeImpl(Iterator<R> schemaInfoIterator) {
    this.schemaInfoIterator = schemaInfoIterator;
  }

  @Override
  public boolean hasNext() {
    return schemaInfoIterator.hasNext();
  }

  @Override
  public R next() {
    return schemaInfoIterator.next();
  }
}
