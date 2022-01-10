package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class StartPipeServerPlan extends PhysicalPlan {

    public StartPipeServerPlan() {
        super(false, Operator.OperatorType.START_PIPE_SERVER);
        canBeSplit = false;
    }

    @Override
    public List<? extends PartialPath> getPaths() {
        return Collections.emptyList();
    }
    @Override
    public void serialize(DataOutputStream stream) throws IOException {
        stream.writeByte((byte) PhysicalPlanType.START_PIPE_SERVER.ordinal());
    }

    @Override
    public void serializeImpl(ByteBuffer buffer) {
        buffer.put((byte) PhysicalPlanType.START_PIPE_SERVER.ordinal());
    }

    @Override
    public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    }
}
