package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.utils.TimeValuePair;

import java.util.Iterator;
import java.util.List;


public interface TimeValuePairSorter {

    /**
     * @return a List which contains all distinct {@link TimeValuePair}s in ascending order by timestamp.
     */
    List<TimeValuePair> getSortedTimeValuePairList();

    /**
     * notice, by default implementation, calling this method will cause calling getSortedTimeValuePairList().
     * @return an iterator of data in this class.
     */
    default Iterator<TimeValuePair> getIterator(){
        return getSortedTimeValuePairList().iterator();
    }


    /**
     * notice, by default implementation, calling this method will cause calling getSortedTimeValuePairList().
     * @return if there is no data in this sorter, return true.
     */
    default boolean isEmpty(){
        return getSortedTimeValuePairList().isEmpty();
    }
}
