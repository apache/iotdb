package org.apache.iotdb.cluster.log;

import java.util.List;

public class StableEntryManager {

    public StableEntryManager(){};

    public List<Log> recover(){
        return null;
    };

    public void append(List<Log> entries){};
}
