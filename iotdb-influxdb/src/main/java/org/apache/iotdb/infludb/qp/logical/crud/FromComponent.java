package org.apache.iotdb.infludb.qp.logical.crud;

import java.util.ArrayList;
import java.util.List;

public class FromComponent {


    private List<String> nameList = new ArrayList<>();

    public FromComponent() {
    }

    public void addNodeName(String name) {
        nameList.add(name);
    }

    public List<String> getNodeName() {
        return nameList;
    }
}
