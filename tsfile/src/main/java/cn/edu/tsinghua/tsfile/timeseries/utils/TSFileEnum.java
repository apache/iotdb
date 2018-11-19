package cn.edu.tsinghua.tsfile.timeseries.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TSFileEnum is similar implementation referring to java enum class. All indexes start from 1 and 0
 * mean null value.
 *
 * @author kangrong
 */
public class TSFileEnum {
    private int index = 1;
    private Map<String, Integer> enumMap = new LinkedHashMap<>();

    public void addTSFileEnum(String value) {
        enumMap.put(value, index++);
    }

    /**
     * just like java enum's ordinal
     *
     * @param value a string appearing in enum.
     * @return the responding index in TSFileEnum
     */
    public int enumOrdinal(String value) {
        return enumMap.getOrDefault(value, -1);
    }

    /**
     * just like java enum's values()
     *
     * @return all values in TSFileEnum in form of List{@code<String>}
     */
    public List<String> getEnumDataValues() {
        return new ArrayList<>(enumMap.keySet());
    }

    @Override
    public String toString() {
        return enumMap.keySet().toString();
    }
}
