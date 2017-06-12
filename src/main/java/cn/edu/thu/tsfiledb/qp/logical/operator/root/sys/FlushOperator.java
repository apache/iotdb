package cn.edu.thu.tsfiledb.qp.logical.operator.root.sys;

import cn.edu.thu.tsfiledb.qp.logical.operator.root.RootOperator;

/**
 *  This class is used for Flush clause(not used up to now).
 */
public class FlushOperator extends RootOperator{

	public FlushOperator(int tokenIntType) {
		super(tokenIntType);
		operatorType = OperatorType.FlUSH;
	}

}
