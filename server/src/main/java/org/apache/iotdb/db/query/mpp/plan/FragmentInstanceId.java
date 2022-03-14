package org.apache.iotdb.db.query.mpp.plan;

public class FragmentInstanceId {
    private String id;
    public FragmentInstanceId(String id) {
        this.id = id;
    }

    //A SinkOperator is needed here. So that we can know where the result of this instance can be sent
}
