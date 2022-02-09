/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package apache.iotdb.quality.validity;

import java.util.ArrayList;

import apache.iotdb.quality.validity.NoNumberException;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.eclipse.collections.api.tuple.primitive.LongIntPair;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;


public class Util {

    /**
     * 从Row中取出指定位置的值，并转化为double类型。注意，必须保证Row中不存在null。
     *
     * @param row 数据行
     * @param index 指定位置的索引
     * @return Row中的指定位置的值
     * @throws NoNumberException Row中的指定位置的值是非数值类型
     */
    public static double getValueAsDouble(Row row, int index) throws NoNumberException {
        double ans = 0;
        switch (row.getDataType(index)) {
            case INT32:
                ans = row.getInt(index);
                break;
            case INT64:
                ans = row.getLong(index);
                break;
            case FLOAT:
                ans = row.getFloat(index);
                break;
            case DOUBLE:
                ans = row.getDouble(index);
                break;
            default:
                throw new NoNumberException();
        }
        return ans;
    }

    /**
     * 从Row中取出第0列的值，并转化为double类型。注意，必须保证Row中不存在null。
     *
     * @param row 数据行
     * @return Row中的第0列的值
     * @throws NoNumberException Row中的第0列的值是非数值类型
     */
    public static double getValueAsDouble(Row row) throws NoNumberException {
        return getValueAsDouble(row, 0);
    }

    /**
     * 从Row中取出第一个值，并转化为Object类型
     *
     * @param row
     * @return Row中的第一个值
     */
    public static Object getValueAsObject(Row row) {
        Object ans = 0;
        switch (row.getDataType(0)) {
            case INT32:
                ans = row.getInt(0);
                break;
            case INT64:
                ans = row.getLong(0);
                break;
            case FLOAT:
                ans = row.getFloat(0);
                break;
            case DOUBLE:
                ans = row.getDouble(0);
                break;
            case BOOLEAN:
                ans = row.getBoolean(0);
                break;
            case TEXT:
                ans = row.getString(0);
                break;
        }
        return ans;
    }

    /**
     * 向PointCollector中加入新的数据点
     *
     * @param pc PointCollector
     * @param type 数据类型
     * @param t 时间戳
     * @param o Object类型的值
     * @throws Exception
     */
    public static void putValue(PointCollector pc, TSDataType type, long t, Object o) throws Exception {
        switch (type) {
            case INT32:
                pc.putInt(t, (Integer) o);
                break;
            case INT64:
                pc.putLong(t, (Long) o);
                break;
            case FLOAT:
                pc.putFloat(t, (Float) o);
                break;
            case DOUBLE:
                pc.putDouble(t, (Double) o);
                break;
            case TEXT:
                pc.putString(t, (String) o);
                break;
            case BOOLEAN:
                pc.putBoolean(t, (Boolean) o);
        }
    }

    /**
     * 将{@code ArrayList<Double>}转化为长度相同的{@code double[]}。
     * <p>
     * 用户需要保证{@code ArrayList<Double>}中没有空值{@code  null}
     *
     * @param list 待转化的{@code ArrayList<Double>}
     * @return 转化后的{@code double[]}
     */
    public static double[] toDoubleArray(ArrayList<Double> list) {
        int len = list.size();
        double ans[] = new double[len];
        for (int i = 0; i < len; i++) {
            ans[i] = list.get(i);
        }
        return ans;
    }

    /**
     * 将{@code ArrayList<Long>}转化为长度相同的{@code long[]}。
     * <p>
     * 用户需要保证{@code ArrayList<Long>}中没有空值{@code  null}
     *
     * @param list 待转化的{@code ArrayList<Long>}
     * @return 转化后的{@code long[]}
     */
    public static long[] toLongArray(ArrayList<Long> list) {
        int len = list.size();
        long ans[] = new long[len];
        for (int i = 0; i < len; i++) {
            ans[i] = list.get(i);
        }
        return ans;
    }

    /**
     * 计算序列的绝对中位差MAD。为了达到渐进正态性，乘上比例因子1.4826。
     * <br>
     * 备注: 1.4826 = 1/qnorm(3/4)
     *
     * @param value 序列
     * @return 绝对中位差MAD
     */
    public static double mad(double[] value) {
        Median median = new Median();
        double mid = median.evaluate(value);
        double d[] = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            d[i] = Math.abs(value[i] - mid);
        }
        return 1.4826 * median.evaluate(d);
    }

    /**
     * 计算序列的取值变化
     *
     * @param origin 原始序列
     * @return 取值变化序列
     */
    public static double[] variation(double origin[]) {
        int n = origin.length;
        double var[] = new double[n - 1];
        for (int i = 0; i < n - 1; i++) {
            var[i] = origin[i + 1] - origin[i];
        }
        return var;
    }

    /**
     * 计算序列的取值变化
     *
     * @param origin 原始序列
     * @return 取值变化序列
     */
    public static double[] variation(long origin[]) {
        int n = origin.length;
        double var[] = new double[n - 1];
        for (int i = 0; i < n - 1; i++) {
            var[i] = origin[i + 1] - origin[i];
        }
        return var;
    }
    
        /**
     * 计算序列的取值变化
     *
     * @param origin 原始序列
     * @return 取值变化序列
     */
    public static int[] variation(int origin[]) {
        int n = origin.length;
        int var[] = new int[n - 1];
        for (int i = 0; i < n - 1; i++) {
            var[i] = origin[i + 1] - origin[i];
        }
        return var;
    }

    /**
     * 计算时间序列的速度
     *
     * @param origin 值序列
     * @param time 时间戳序列
     * @return 速度序列
     */
    public static double[] speed(double origin[], double time[]) {
        int n = origin.length;
        double speed[] = new double[n - 1];
        for (int i = 0; i < n - 1; i++) {
            speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
        }
        return speed;
    }

    /**
     * 计算时间序列的速度
     *
     * @param origin 值序列
     * @param time 时间戳序列
     * @return 速度序列
     */
    public static double[] speed(double origin[], long time[]) {
        int n = origin.length;
        double speed[] = new double[n - 1];
        for (int i = 0; i < n - 1; i++) {
            speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
        }
        return speed;
    }

    /**
     * 计算序列的众数
     *
     * @param values 序列
     * @return 众数
     */
    public static long mode(long[] values) {
        LongIntHashMap map = new LongIntHashMap();
        for (long v : values) {
            map.addToValue(v, 1);
        }
        long key = 0;
        int maxValue = 0;
        for (LongIntPair p : map.keyValuesView()) {
            if (p.getTwo() > maxValue) {
                key = p.getOne();
                maxValue = p.getTwo();
            }
        }
        return key;
    }

    public static long parseTime(String s) {
        long unit = 0;
        s = s.toLowerCase();
        s = s.replaceAll(" ", "");
        if (s.endsWith("ms")) {
            unit = 1;
            s = s.substring(0, s.length() - 2);
        } else if (s.endsWith("s")) {
            unit = 1000;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("m")) {
            unit = 60 * 1000;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("h")) {
            unit = 60 * 60 * 1000;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("d")) {
            unit = 24 * 60 * 60 * 1000;
            s = s.substring(0, s.length() - 1);
        }
        double v = Double.parseDouble(s);
        return (long) (unit * v);
    }

}
