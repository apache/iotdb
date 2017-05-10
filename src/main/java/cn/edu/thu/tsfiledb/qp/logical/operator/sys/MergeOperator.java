package cn.edu.thu.tsfiledb.qp.logical.operator.sys;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.RootOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.SFWOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

public class MergeOperator extends RootOperator{

	public MergeOperator(int tokenIntType) {
		super(tokenIntType);
		// TODO Auto-generated constructor stub
	}

	@Override
	public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf) throws QueryProcessorException {
		// TODO Auto-generated method stub
		return null;
	}
}
