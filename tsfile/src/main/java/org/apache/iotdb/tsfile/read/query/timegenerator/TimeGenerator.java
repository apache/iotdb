package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.Path;

import java.io.IOException;

/**
 * All SingleSeriesExpression involved in a IExpression will be transferred to a TimeGenerator tree whose leaf nodes are all SeriesReaders,
 * The TimeGenerator tree can generate the next timestamp that satisfies the filter condition.
 *
 * Then we use this timestamp to get values in other series that are not included in IExpression
 */
public interface TimeGenerator {

    boolean hasNext() throws IOException;

    long next() throws IOException;

    Object getValue(Path path, long time) throws IOException;

}
