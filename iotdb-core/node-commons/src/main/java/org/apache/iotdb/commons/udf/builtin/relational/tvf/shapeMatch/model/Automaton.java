package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model;

import java.util.LinkedList;
import java.util.Queue;

public class Automaton {
    private Section startSection = null;
    private int stateNum = 0;

    public void setStartSection(Section startSection) {
        this.startSection = startSection;
    }

    public Section getStartSection() {
        return startSection;
    }

    public void setStateNum(int stateNum) {
        this.stateNum = stateNum;
    }
}
