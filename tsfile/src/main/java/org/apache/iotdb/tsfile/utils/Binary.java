package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Override compareTo() and equals() function to Binary class. This class is
 * used to accept Java String type
 *
 *
 * @author xuyi
 */
public class Binary implements Comparable<Binary>, Serializable {
    private static final long serialVersionUID = 6394197743397020735L;

    public byte[] values;
    private String textEncodingType = TSFileConfig.STRING_ENCODING;

    /**
     * if the bytes v is modified, the modification is visable to this binary.
     * @param v
     */
    public Binary(byte[] v) {
        this.values = v;
    }

    public Binary(String s) {
        this.values = (s == null) ? null : s.getBytes(Charset.forName(this.textEncodingType));
    }

    public static Binary valueOf(String value) {
        return new Binary(BytesUtils.StringToBytes(value));
    }

    @Override
    public int compareTo(Binary other) {
        if (other == null) {
            if (this.values == null) {
                return 0;
            } else {
                return 1;
            }
        }

        int i = 0;
        while (i < getLength() && i < other.getLength()) {
            if (this.values[i] == other.values[i]) {
                i++;
                continue;
            }
            return this.values[i] - other.values[i];
        }
        return getLength() - other.getLength();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (getClass() != other.getClass())
            return false;

        if (compareTo((Binary) other) == 0)
            return true;
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    public int getLength() {
        if (this.values == null)
            return -1;
        return this.values.length;
    }

    public String getStringValue() {
        return new String(this.values, Charset.forName(this.textEncodingType));
    }

    public String getTextEncodingType() {
        return textEncodingType;
    }

    public void setTextEncodingType(String textEncodingType) {
        this.textEncodingType = textEncodingType;
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    public byte[] getValues() {
        return values;
    }
}
