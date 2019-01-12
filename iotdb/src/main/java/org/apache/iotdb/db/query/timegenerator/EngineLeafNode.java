package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.NodeType;

import java.io.IOException;


public class EngineLeafNode implements Node {

    private IReader reader;

    private BatchData data = null;

    private boolean gotData = false;

    public EngineLeafNode(IReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean hasNext() throws IOException {

        return reader.hasNext();

//        if (gotData) {
//            data.next();
//            gotData = false;
//        }
//
//        if (data == null || !data.hasNext()) {
//            if (reader.hasNextBatch())
//                data = reader.nextBatch();
//            else
//                return false;
//        }
//
//        return data.hasNext();
    }

    @Override
    public long next() throws IOException {
        return reader.next().getTimestamp();

//        long time = data.currentTime();
//        gotData = true;
//        return time;
    }

    public boolean currentTimeIs(long time) {
        if(!reader.currentBatch().hasNext()) {
            return false;
        }
        return reader.currentBatch().currentTime() == time;
    }

    public Object currentValue(long time) {
        if(data.currentTime() == time) {
            return data.currentValue();
        }
        return null;
    }

    @Override
    public NodeType getType() {
        return NodeType.LEAF;
    }


}
