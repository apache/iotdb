package org.apache.iotdb.db.metadata.metafile;

public interface PersistenceInfo {

    long getPosition();

    void setPosition(long position);

}
