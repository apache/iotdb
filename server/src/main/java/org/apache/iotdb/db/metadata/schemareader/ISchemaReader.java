package org.apache.iotdb.db.metadata.schemareader;

import java.util.Iterator;

public abstract class ISchemaReader<R> implements Iterator<R> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public abstract R next() ;

}
