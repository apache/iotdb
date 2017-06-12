package cn.edu.thu.tsfiledb.qp.logical.root.sys;

import cn.edu.thu.tsfiledb.qp.logical.root.RootOperator;

/**
 *  This class is used for Merge clause(not used up to now).
 */
public class MergeOperator extends RootOperator{

	public MergeOperator(int tokenIntType) {
		super(tokenIntType);
		operatorType = OperatorType.MERGE;
	}

}
