package org.apache.iotdb.db.qp.constant;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.sql.parse.TSParser;
import org.apache.iotdb.db.sql.parse.TSParser;


public class TSParserConstant {
    private static Map<Integer, Integer> antlrQpMap = new HashMap<>();

    //used to get operator type when construct operator from AST Tree
    static{
        antlrQpMap.put(TSParser.KW_AND, SQLConstant.KW_AND);
        antlrQpMap.put(TSParser.KW_OR, SQLConstant.KW_OR);
        antlrQpMap.put(TSParser.KW_NOT, SQLConstant.KW_NOT);
        
        antlrQpMap.put(TSParser.EQUAL, SQLConstant.EQUAL);
        antlrQpMap.put(TSParser.NOTEQUAL, SQLConstant.NOTEQUAL);
        antlrQpMap.put(TSParser.LESSTHANOREQUALTO, SQLConstant.LESSTHANOREQUALTO);
        antlrQpMap.put(TSParser.LESSTHAN, SQLConstant.LESSTHAN);
        antlrQpMap.put(TSParser.LESSTHANOREQUALTO, SQLConstant.LESSTHANOREQUALTO);
        antlrQpMap.put(TSParser.LESSTHAN, SQLConstant.LESSTHAN);
        antlrQpMap.put(TSParser.GREATERTHANOREQUALTO, SQLConstant.GREATERTHANOREQUALTO);
        antlrQpMap.put(TSParser.GREATERTHAN, SQLConstant.GREATERTHAN);
        antlrQpMap.put(TSParser.EQUAL_NS, SQLConstant.EQUAL_NS);
        
        antlrQpMap.put(TSParser.TOK_SELECT, SQLConstant.TOK_SELECT);
        antlrQpMap.put(TSParser.TOK_FROM, SQLConstant.TOK_FROM);
        antlrQpMap.put(TSParser.TOK_WHERE, SQLConstant.TOK_WHERE);
        antlrQpMap.put(TSParser.TOK_QUERY, SQLConstant.TOK_QUERY);
    }
    
    public static int getTSTokenIntType(int antlrIntType) {
        if(!antlrQpMap.containsKey(antlrIntType)) {
            System.out.println(antlrIntType);
        }
        return antlrQpMap.get(antlrIntType);
    }
    
}
