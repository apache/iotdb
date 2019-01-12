package org.apache.iotdb.tsfile.read.query.timegenerator.node;

import java.io.IOException;


public interface Node {

    boolean hasNext() throws IOException;

    long next() throws IOException;

    NodeType getType();
}
