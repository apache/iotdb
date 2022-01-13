package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Iterator;

public interface TimeSeries extends Iterator<Object[]> {

  TSDataType[] getSpecification();

}
