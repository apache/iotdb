package cn.edu.tsinghua.iotdb.engine.filenode;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.DoubleFilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FloatFilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.IntFilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.LongFilterSeries;

public class FilterUtilsForOverflow {

	public static SingleSeriesFilterExpression construct(String exp) {
		if (exp.equals("null")) {
			return null;
		}
		String args[] = exp.split(",");
		if (args[0].equals("0") || args[0].equals("1")) {
			return construct("null", "null", args[0], args[1]);
		}
		return construct(args[1], args[2], args[0], args[3]);

	}

	public static SingleSeriesFilterExpression construct(String device, String sensor, String filterType, String exp) {

		if (exp.equals("null")) {
			return null;
		}
		if (exp.charAt(0) != '(') {
			boolean ifEq = exp.charAt(1) == '=' ? true : false;
			FilterSeriesType type = FilterSeriesType.TIME_FILTER;
			int offset = ifEq ? 2 : 1;
			if (exp.charAt(0) == '=') {
				if (type == FilterSeriesType.TIME_FILTER) {
					long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
					return FilterFactory.eq(FilterFactory.longFilterSeries(device, sensor, type), v);
				} else if (type == FilterSeriesType.FREQUENCY_FILTER) {
					float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
					return FilterFactory.eq(FilterFactory.floatFilterSeries(device, sensor, type), v);
				} else {
					FilterSeries<?> col = getColumn(device, sensor);
					if (col instanceof IntFilterSeries) {
						int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.eq(FilterFactory.intFilterSeries(device, sensor, type), v);
					} else if (col instanceof LongFilterSeries) {
						long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.eq(FilterFactory.longFilterSeries(device, sensor, type), v);
					} else if (col instanceof FloatFilterSeries) {
						float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.eq(FilterFactory.floatFilterSeries(device, sensor, type), v);
					} else if (col instanceof DoubleFilterSeries) {
						double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.eq(FilterFactory.doubleFilterSeries(device, sensor, type), v);
					} else {
						return null;
					}

				}
			} else if (exp.charAt(0) == '>') {
				if (type == FilterSeriesType.TIME_FILTER) {
					long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
					return FilterFactory.gtEq(FilterFactory.longFilterSeries(device, sensor, type), v, ifEq);
				} else if (type == FilterSeriesType.FREQUENCY_FILTER) {
					float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
					return FilterFactory.gtEq(FilterFactory.floatFilterSeries(device, sensor, type), v, ifEq);
				} else {
					FilterSeries<?> col = getColumn(device, sensor);
					if (col instanceof IntFilterSeries) {
						int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.gtEq(FilterFactory.intFilterSeries(device, sensor, type), v, ifEq);
					} else if (col instanceof LongFilterSeries) {
						long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.gtEq(FilterFactory.longFilterSeries(device, sensor, type), v, ifEq);
					} else if (col instanceof FloatFilterSeries) {
						float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.gtEq(FilterFactory.floatFilterSeries(device, sensor, type), v, ifEq);
					} else if (col instanceof DoubleFilterSeries) {
						double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.gtEq(FilterFactory.doubleFilterSeries(device, sensor, type), v, ifEq);
					} else {
						return null;
					}

				}
			} else if (exp.charAt(0) == '<') {
				if (type == FilterSeriesType.TIME_FILTER) {
					long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
					return FilterFactory.ltEq(FilterFactory.longFilterSeries(device, sensor, type), v, ifEq);
				} else if (type == FilterSeriesType.FREQUENCY_FILTER) {
					float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
					return FilterFactory.ltEq(FilterFactory.floatFilterSeries(device, sensor, type), v, ifEq);
				} else {

					FilterSeries<?> col = getColumn(device, sensor);
					if (col instanceof IntFilterSeries) {
						int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.ltEq(FilterFactory.intFilterSeries(device, sensor, type), v, ifEq);
					} else if (col instanceof LongFilterSeries) {
						long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.ltEq(FilterFactory.longFilterSeries(device, sensor, type), v, ifEq);
					} else if (col instanceof FloatFilterSeries) {
						float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.ltEq(FilterFactory.floatFilterSeries(device, sensor, type), v, ifEq);
					} else if (col instanceof DoubleFilterSeries) {
						double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
						return FilterFactory.ltEq(FilterFactory.doubleFilterSeries(device, sensor, type), v, ifEq);
					} else {
						return null;
					}
				}
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
				SingleSeriesFilterExpression filter = construct(device, sensor, filterType,
						texp.substring(1, texp.length() - 1));
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
			filters.add(construct(device, sensor, filterType, texp.substring(1, texp.length() - 1)));
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

	public static FilterExpression constructCrossFilter(String exp) {
		exp = exp.trim();

		if (exp.equals("null")) {
			return null;
		}

		if (exp.charAt(0) != '[') {
			return construct(exp);
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
				FilterExpression filter = constructCrossFilter(texp.substring(1, texp.length() - 1));
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
			filters.add(constructCrossFilter(texp.substring(1, texp.length() - 1)));
		}

		if (operators.size() == 0) {
			// Warning TODO
			return FilterFactory.csAnd(filters.get(0), filters.get(0));
		}

		CrossSeriesFilterExpression csf;
		if (operators.get(0) == '|') {
			csf = (CrossSeriesFilterExpression) FilterFactory.or(filters.get(0), filters.get(1));
		} else {
			csf = FilterFactory.csAnd(filters.get(0), filters.get(1));
		}

		for (int i = 2; i < filters.size(); i++) {
			if (operators.get(i - 1) == '|') {
				csf = (CrossSeriesFilterExpression) FilterFactory.or(csf, filters.get(i));
			} else {
				csf = FilterFactory.csAnd(csf, filters.get(i));
			}
		}
		return csf;
	}

	public static FilterSeries<?> getColumn(String deltaObjectUID, String measurementID) {
		TSDataType type;
		try {
			type = MManager.getInstance().getSeriesType(deltaObjectUID + "." + measurementID);
			if (type == TSDataType.INT32) {
				return FilterFactory.intFilterSeries(deltaObjectUID, measurementID, FilterSeriesType.VALUE_FILTER);
			} else if (type == TSDataType.INT64) {
				return FilterFactory.longFilterSeries(deltaObjectUID, measurementID, FilterSeriesType.VALUE_FILTER);
			} else if (type == TSDataType.FLOAT) {
				return FilterFactory.floatFilterSeries(deltaObjectUID, measurementID, FilterSeriesType.VALUE_FILTER);
			} else if (type == TSDataType.DOUBLE) {
				return FilterFactory.doubleFilterSeries(deltaObjectUID, measurementID, FilterSeriesType.VALUE_FILTER);
			} else if (type == TSDataType.BOOLEAN) {
				return FilterFactory.booleanFilterSeries(deltaObjectUID, measurementID, FilterSeriesType.VALUE_FILTER);
			}
		} catch (PathErrorException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String args[]) {
		String exp = "[[2,device1,sensor1,(>10)&(<100)]&[2,device2,sensor2,(>100)|(<102)]]| [2,device2,sensor2,(>100)|(<102)]";
		CrossSeriesFilterExpression csf = (CrossSeriesFilterExpression) constructCrossFilter(exp);
		System.out.println(csf);
	}
}
