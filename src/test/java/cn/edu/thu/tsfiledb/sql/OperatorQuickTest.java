package cn.edu.thu.tsfiledb.sql;

import cn.edu.thu.tsfiledb.qp.strategy.LogicalGenerator;
import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.ParseUtils;

public class OperatorQuickTest{
	
	public static void main(String args[]) throws Exception
	{
		
//		String SqlStr = "delete from d1.s1 where time < (2016-11-16 16:22:33:75)";
//		String SqlStr = "insert into root.vehicle.d0 (timestamp,s0 )  values((2016-11-16 16:22:33:75),1011)";
//		String SqlStr = "insert into root.vehicle.d0 (timestamp,  s0 )  values(1234567 , 1011)";
		String SqlStr = "select s1, s2 from root.vehicle.d1 where time < 10";
		LogicalGenerator tPlan = new LogicalGenerator();
		
		
		ASTNode asTree = ParseGenerator.generateAST(SqlStr);
		asTree = ParseUtils.findRootNonNullToken(asTree);
		System.out.println("hello test world");
		System.out.println(asTree.dump());
		tPlan.analyze(asTree);
		System.out.println("hello test world");
	}	
}