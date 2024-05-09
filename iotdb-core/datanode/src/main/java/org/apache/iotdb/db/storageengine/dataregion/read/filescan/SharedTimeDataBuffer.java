package org.apache.iotdb.db.storageengine.dataregion.read.filescan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SharedTimeDataBuffer {
    ByteBuffer timeBuffer;
    List<Long[]> timeData;

    int pageId;

    public SharedTimeDataBuffer(ByteBuffer timeBuffer) {
        this.timeBuffer = timeBuffer;
        this.timeData = new ArrayList<>();
        this.pageId = 0;
    }

    public synchronized Long[] getPageData(int pageId){
        int size = timeData.size();
        if(pageId < size){
            return timeData.get(pageId);
        } else if(pageId == size){
            loadPageData();
            return timeData.get(pageId);
        } else {
            throw new UnsupportedOperationException("PageId in SharedTimeDataBuffer should be  incremental.");
        }
    }

    private void loadPageData(){
        if(timeBuffer.hasRemaining()){
            int size = timeBuffer.getInt();
            Long[] pageData = new Long[size];
            for(int i = 0; i < size; i++){
                pageData[i] = timeBuffer.getLong();
            }
            timeData.add(pageData);
        }else {
            throw new UnsupportedOperationException("No more data in SharedTimeDataBuffer");
        }
    }


}
