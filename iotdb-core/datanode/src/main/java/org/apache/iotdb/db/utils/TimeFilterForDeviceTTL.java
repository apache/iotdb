package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.filter.basic.Filter;

import java.util.Map;

public class TimeFilterForDeviceTTL {

    private Filter timeFilter;

    private Map<IDeviceID, Long> ttlCached;

    public TimeFilterForDeviceTTL(Filter timeFilter) {
        this.timeFilter = timeFilter;
    }

    public boolean satisfyStartEndTIme(long startTime, long endTime, IDeviceID deviceID){
        long ttl = getTTL(deviceID);
        if(ttl != Long.MAX_VALUE){
            long validStartTime = CommonDateTimeUtils.currentTime() - ttl;
            if(validStartTime > endTime){
                return false;
            }
            return timeFilter.satisfyStartEndTime(validStartTime, endTime);
        }
        return timeFilter.satisfyStartEndTime(startTime, endTime);
    }

    public boolean satisfy(long time, IDeviceID deviceID){
        long ttl = getTTL(deviceID);
        if(ttl != Long.MAX_VALUE) {
            long validStartTime = CommonDateTimeUtils.currentTime() - ttl;
            if(validStartTime > time){
                return false;
            }
            return timeFilter.satisfy(validStartTime, null);
        }
        return timeFilter.satisfy(time, null);
    }

    private long getTTL(IDeviceID deviceID){
        if(ttlCached.containsKey(deviceID)){
            return ttlCached.get(deviceID);
        }
        long ttl = DataNodeTTLCache.getInstance().getTTL(deviceID);
        ttlCached.put(deviceID, ttl);
        return ttl;
    }


}
