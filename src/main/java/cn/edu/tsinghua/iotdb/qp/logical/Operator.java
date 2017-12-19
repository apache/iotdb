package cn.edu.tsinghua.iotdb.qp.logical;

import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;

/**
 * This class is a superclass of all operator.
 *
 * @author kangrong
 * @author qiaojialin
 *
 */
public abstract class Operator {

	//operator type in int format
	protected int tokenIntType;
	//operator type in String format
	protected String tokenName;

	protected OperatorType operatorType = OperatorType.NULL;

	public Operator(int tokenIntType) {
		this.tokenIntType = tokenIntType;
		this.tokenName = SQLConstant.tokenNames.get(tokenIntType);
	}

	public OperatorType getType() {
		return operatorType;
	}

	public boolean isQuery() {
		return operatorType == OperatorType.QUERY;
	}

	public int getTokenIntType() {
		return tokenIntType;
	}

	public String getTokenName() {
		return tokenName;
	}

	public void setOperatorType(OperatorType operatorType) {
		this.operatorType = operatorType;
	}

	@Override
	public String toString() {
		return tokenName;
	}

	/**
	 * If you want to add new OperatorType, you must add it in the last!
	 */
	public enum OperatorType {
		SFW, JOIN, UNION, FILTER, GROUPBY, ORDERBY, LIMIT, SELECT, SEQTABLESCAN, HASHTABLESCAN, MERGEJOIN, FILEREAD, NULL, TABLESCAN,
		UPDATE, INSERT, DELETE, BASIC_FUNC, QUERY, MERGEQUERY, AGGREGATION, AUTHOR, FROM, FUNC, LOADDATA, METADATA, PROPERTY,
		OVERFLOWFLUSHSTART,
		OVERFLOWFLUSHEND,
		BUFFERFLUSHSTART,
		BUFFERFLUSHEND,
		INDEX;
	}
}
