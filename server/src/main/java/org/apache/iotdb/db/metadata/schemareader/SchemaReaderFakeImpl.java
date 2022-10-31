package org.apache.iotdb.db.metadata.schemareader;

import org.apache.iotdb.db.metadata.schemainfo.ISchemaInfo;

import java.util.Iterator;

public class SchemaReaderFakeImpl extends ISchemaReader<ISchemaInfo> {
    private Iterator<? extends ISchemaInfo> schemaInfoIterator;

    @Override
    public boolean hasNext() {
        return schemaInfoIterator.hasNext();
    }

    @Override
    public ISchemaInfo next() {
        return schemaInfoIterator.next();
    }
}
