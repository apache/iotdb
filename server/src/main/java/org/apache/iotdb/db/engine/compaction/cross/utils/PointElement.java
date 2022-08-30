package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

public class PointElement {
    public long timestamp;
    public int priority;
    public TimeValuePair timeValuePair;
    public TsBlock.TsBlockSingleColumnIterator page;
    public PageElement pageElement;

    public PointElement(PageElement pageElement){
        this.pageElement=pageElement;
        this.page=pageElement.batchData.getTsBlockSingleColumnIterator();
        this.timeValuePair=page.currentTimeValuePair();
        this.timestamp=timeValuePair.getTimestamp();
        this.priority=pageElement.priority;
    }

    public void setPoint(TimeValuePair timeValuePair){
        this.timeValuePair=timeValuePair;
        this.timestamp=timeValuePair.getTimestamp();
    }
}
