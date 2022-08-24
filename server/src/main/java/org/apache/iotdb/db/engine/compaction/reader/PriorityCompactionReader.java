package org.apache.iotdb.db.engine.compaction.reader;

import org.apache.iotdb.db.engine.compaction.cross.utils.PointDataElement;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.util.Comparator;
import java.util.List;

public class PriorityCompactionReader {
    private final BatchData[] pageDatas;

    private final int[] priority;

    private long lastTime;

    private long[] curTime;


    public PriorityCompactionReader(List<PointDataElement> pointDataElements){
        pointDataElements.sort(Comparator.comparingLong(x -> x.batchData.currentTime()));
        priority=new int[pointDataElements.size()];
        pageDatas=new BatchData[pointDataElements.size()];
        curTime=new long[pointDataElements.size()];
        for(int i=0;i<pointDataElements.size();i++){
            PointDataElement pointDataElement=pointDataElements.get(i);
            pageDatas[i]=pointDataElement.batchData;
            priority[i]=pointDataElement.priority;
        }
        // initial current timestamp
        for(int i=0;i<pageDatas.length;i++){
           curTime[i]=pageDatas[i].currentTime();
        }
    }

    public TimeValuePair nextPoint(){
        TimeValuePair timeValuePair;
        // get the highest priority point
        int highestPriorityPointIndex=0;
        for(int i=highestPriorityPointIndex+1;i<curTime.length;i++){
            if(curTime[i]<curTime[highestPriorityPointIndex]){
                // small time has higher priority
                highestPriorityPointIndex=i;
            }else if(curTime[i]==curTime[highestPriorityPointIndex]&&priority[i]>priority[highestPriorityPointIndex]){
                // if time equals, newer point has higher priority
                highestPriorityPointIndex=i;
            }
        }
        timeValuePair= TimeValuePairUtils.getCurrentTimeValuePair(pageDatas[highestPriorityPointIndex]);
        lastTime=curTime[highestPriorityPointIndex];

        // remove data points with the same timestamp as the last point
        for(int i=0;i<pageDatas.length;i++){
            if(curTime[i]>lastTime){
                continue;
            }
            BatchData batchData=pageDatas[i];
            if(i==highestPriorityPointIndex){
                batchData.next();
                curTime[i]=batchData.currentTime();
                continue;
            }
            while (batchData.currentTime()<=lastTime){
                batchData.next();
            }
            curTime[i]=batchData.currentTime();
        }
        return timeValuePair;
    }



    public void addNewPage()

}
