package org.apache.iotdb.cluster.log;

public class HardState {
    public long currentTerm;
    public long voteFor;

    public HardState(long currentTerm,long voteFor){
        this.currentTerm = currentTerm;
        this.voteFor = voteFor;
    }
}
