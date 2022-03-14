package org.apache.iotdb.db.query.mpp.plan;

public class FragmentInstance {
    private FragmentInstanceId id;

    // The reference of PlanFragment which this instance is generated from
    private PlanFragment fragment;

    // We can add some more params for a specific FragmentInstance
    // So that we can make different FragmentInstance owns different data range.
}
