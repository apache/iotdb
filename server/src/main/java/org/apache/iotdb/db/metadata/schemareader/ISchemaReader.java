package org.apache.iotdb.db.metadata.schemareader;

import org.apache.iotdb.db.metadata.schemainfo.ISchemaInfo;

import java.util.Iterator;

public abstract class ISchemaReader<R extends ISchemaInfo> implements Iterator<R> {

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract R next() ;

}
