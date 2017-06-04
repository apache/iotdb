package cn.edu.thu.tsfiledb.sql.exec;

import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.ParseUtils;

public class OperatorQuickTest{
	
	public static void main(String args[]) throws Exception
	{
		
		String SqlStr = "UPDATE USER 'user1221' WHERE 'passowrd2' TO 'username222'";
		TSPlanContextV2 tPlan = new TSPlanContextV2();
		
		
		ASTNode asTree = ParseGenerator.generateAST(SqlStr);
		asTree = ParseUtils.findRootNonNullToken(asTree);
		System.out.println("hello test world");
		System.out.println(asTree.dump());
		tPlan.analyze(asTree);
		System.out.println("hello test world");
	}	
}




/*Reference Code:
        ASTNode astTree = ParseGenerator.generateAST(sqlStr);
        astTree = ParseUtils.findRootNonNullToken(astTree);
        System.out.println(astTree.dump());


----------------------
    private void analyzeAuthorCreate(ASTNode astNode, int tokenIntType)
            throws IllegalASTFormatException {
        int childCount = astNode.getChildCount();
        if (childCount == 2) {
            // create user
            authorOperator =
                    new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE, AuthorType.CREATE_USER);
            authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
            authorOperator.setPassWord(astNode.getChild(1).getChild(0).getText());
        } else if (childCount == 1) {
            // create role
            authorOperator =
                    new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE, AuthorType.CREATE_ROLE);
            authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
        } else {
            throw new IllegalASTFormatException("illegal ast tree in create author command:\n"
                    + astNode.dump());
        }
        initialiedOperator = authorOperator;
    }
 * 
 * 
 * 
 * 
 * */
