package cn.edu.tsinghua.tsfile.timeseries.filter.utils;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.CSAnd;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.CSOr;
import cn.edu.tsinghua.tsfile.timeseries.read.RecordReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterUtils {

    private static final char PATH_SPLITER = '.';

    //exp-format:deltaObject,measurement,type,exp
    public static SingleSeriesFilterExpression construct(String exp, RecordReader recordReader) throws IOException{
        if (exp == null || exp.equals("null")) {
            return null;
        }
        String args[] = exp.split(",");
        if (args[0].equals("0") || args[0].equals("1")) {
            return construct("null", "null", args[0], args[1], recordReader);
        }
        String s = args[1];
        String deltaObject = s.substring(0, s.lastIndexOf(PATH_SPLITER));
        String measurement = s.substring(s.lastIndexOf(PATH_SPLITER) + 1);
        return construct(deltaObject, measurement, args[0], args[2], recordReader);

    }

    public static SingleSeriesFilterExpression construct(String deltaObject, String measurement, String filterType,
                                                         String exp, RecordReader recordReader) throws IOException{

        if (exp.equals("null")) {
            return null;
        }
        if (exp.charAt(0) != '(') {
            boolean ifEq = exp.charAt(1) == '=' ? true : false;
            int type = Integer.valueOf(filterType);
            int offset = ifEq ? 2 : 1;
            if (exp.charAt(0) == '=') {
                if (type == 0) {
                    long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.eq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.TIME_FILTER), v);
                } else if (type == 1) {
                    float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.eq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.FREQUENCY_FILTER), v);
                } else {
                    if (recordReader == null) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    }
                    FilterSeries<?> col = recordReader.getColumnByMeasurementName(deltaObject, measurement);
                    if (col instanceof IntFilterSeries) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof BooleanFilterSeries) {
                        boolean v = Boolean.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.booleanFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof LongFilterSeries) {
                        long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof FloatFilterSeries) {
                        float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof DoubleFilterSeries) {
                        double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof StringFilterSeries) {
                        String v = String.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.stringFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), new Binary(v));
                    } else {
                        throw new UnSupportedDataTypeException("Construct FilterSeries: " + col);
                    }

                }
            } else if (exp.charAt(0) == '>') {
                if (type == 0) {
                    long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.TIME_FILTER), v, ifEq);
                } else if (type == 1) {
                    float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.FREQUENCY_FILTER), v, ifEq);
                } else {
                    if (recordReader == null) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    }
                    FilterSeries<?> col = recordReader.getColumnByMeasurementName(deltaObject, measurement);
                    if (col instanceof IntFilterSeries) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof LongFilterSeries) {
                        long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof FloatFilterSeries) {
                        float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof DoubleFilterSeries) {
                        double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof StringFilterSeries) {
                        String v = String.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.stringFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), new Binary(v), ifEq);
                    } else {
                        throw new UnSupportedDataTypeException("Construct FilterSeries: " + col);
                    }

                }
            } else if (exp.charAt(0) == '<') {
                if (type == 0) {
                    long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.TIME_FILTER), v, ifEq);
                } else if (type == 1) {
                    float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.FREQUENCY_FILTER), v, ifEq);
                } else {
                    //default filter
                    if (recordReader == null) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    }
                    FilterSeries<?> col = recordReader.getColumnByMeasurementName(deltaObject, measurement);
                    if (col instanceof IntFilterSeries) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof LongFilterSeries) {
                        long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof FloatFilterSeries) {
                        float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof DoubleFilterSeries) {
                        double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof StringFilterSeries) {
                        String v = String.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.stringFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), new Binary(v), ifEq);
                    } else {
                        throw new UnSupportedDataTypeException("Construct FilterSeries: " + col);
                    }
                }
                // long v = Long.valueOf(exp.substring(offset,exp.length()).trim());
                // return FilterFactory.ltEq(FilterFactory.longColumn(deltaObject, measurement, ifTime), v, ifEq);
            }
            return null;
        }


        List<Character> operators = new ArrayList<Character>();
        List<SingleSeriesFilterExpression> filters = new ArrayList<>();

        int idx = 0;
        int numbracket = 0;
        boolean ltgtFlag = false;
        boolean operFlag = false;

        String texp = "";

        for (; idx < exp.length(); idx++) {
            char c = exp.charAt(idx);
            if (Character.isWhitespace(c) || c == '\0') {
                continue;
            }
            if (c == '(') {
                numbracket++;
            }
            if (c == ')') {
                numbracket--;
            }
            if (c == '>' || c == '<') {
                ltgtFlag = true;
            }
            if (numbracket == 0 && (c == '|' || c == '&')) {
                operFlag = true;
            }

            if (ltgtFlag && numbracket == 0 && operFlag) {
                SingleSeriesFilterExpression filter = construct(deltaObject, measurement, filterType,
                        texp.substring(1, texp.length() - 1), recordReader);
                filters.add(filter);
                operators.add(c);
                numbracket = 0;
                ltgtFlag = false;
                operFlag = false;
                texp = "";
            } else {
                texp += c;
            }
        }
        if (!texp.equals("")) {
            filters.add(construct(deltaObject, measurement, filterType, texp.substring(1, texp.length() - 1), recordReader));
        }

        if (filters.size() - operators.size() != 1) {
            return null;
        }

        SingleSeriesFilterExpression filter = filters.get(0);
        for (int i = 0; i < operators.size(); i++) {
            if (operators.get(i) == '|') {
                filter = (SingleSeriesFilterExpression) FilterFactory.or(filter, filters.get(i + 1));
            } else if (operators.get(i) == '&') {
                filter = (SingleSeriesFilterExpression) FilterFactory.and(filter, filters.get(i + 1));
            }
        }

        return filter;
    }

    public static FilterExpression constructCrossFilter(String exp, RecordReader recordReader) throws IOException {
        exp = exp.trim();

        if (exp.equals("null")) {
            return null;
        }

        if (exp.charAt(0) != '[') {
            return construct(exp, recordReader);
        }

        int numbraket = 0;
        boolean operator = false;
        ArrayList<FilterExpression> filters = new ArrayList<>();
        ArrayList<Character> operators = new ArrayList<>();
        String texp = "";

        for (int i = 0; i < exp.length(); i++) {
            char c = exp.charAt(i);

            if (Character.isWhitespace(c) || c == '\0') {
                continue;
            }

            if (c == '[') {
                numbraket++;
            }
            if (c == ']') {
                numbraket--;
            }
            if (numbraket == 0 && (c == '|' || c == '&')) {
                operator = true;
            }

            if (numbraket == 0 && operator) {
//    			System.out.println(texp);
//    			System.out.println(texp.length());
                FilterExpression filter = constructCrossFilter(texp.substring(1, texp.length() - 1), recordReader);
                filters.add(filter);
                operators.add(c);

                numbraket = 0;
                operator = false;
                texp = "";
            } else {
                texp += c;
            }
        }
        if (!texp.equals("")) {
            filters.add(constructCrossFilter(texp.substring(1, texp.length() - 1), recordReader));
        }

        if (operators.size() == 0) {
            //Warning TODO
            return new CSAnd(filters.get(0), filters.get(0));
        }

        CrossSeriesFilterExpression csf;
        if (operators.get(0) == '|') {
            csf = new CSOr(filters.get(0), filters.get(1));
        } else {
            csf = new CSAnd(filters.get(0), filters.get(1));
        }

        for (int i = 2; i < filters.size(); i++) {
            if (operators.get(i - 1) == '|') {
                csf = new CSOr(csf, filters.get(i));
            } else {
                csf = new CSAnd(csf, filters.get(i));
            }
        }
        return csf;
    }
}












