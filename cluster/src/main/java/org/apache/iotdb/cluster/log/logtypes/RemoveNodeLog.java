package org.apache.iotdb.cluster.log.logtypes;

import java.io.IOException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.SerializeUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RemoveNodeLog extends Log {
    private Node removedNode;

    @Override
    public ByteBuffer serialize() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        try {
            dataOutputStream.writeByte(Types.REMOVE_NODE.ordinal());
            dataOutputStream.writeLong(getPreviousLogIndex());
            dataOutputStream.writeLong(getPreviousLogTerm());
            dataOutputStream.writeLong(getCurrLogIndex());
            dataOutputStream.writeLong(getCurrLogTerm());
        } catch (IOException e) {
            // ignored
        }
        SerializeUtils.serialize(removedNode, dataOutputStream);
        return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
        setPreviousLogIndex(buffer.getLong());
        setPreviousLogTerm(buffer.getLong());
        setCurrLogIndex(buffer.getLong());
        setCurrLogTerm(buffer.getLong());

        removedNode = new Node();
        SerializeUtils.deserialize(removedNode, buffer);
    }

    public Node getRemovedNode() {
        return removedNode;
    }

    public void setRemovedNode(Node removedNode) {
        this.removedNode = removedNode;
    }
}
