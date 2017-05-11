package cn.edu.thu.tsfiledb.sql.exec;

import java.util.HashMap;
import java.util.Map;

import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.sql.parse.TSParser;

public class TSParserConstant {
    public static Map<Integer, Integer> anltrQpMap = new HashMap<Integer, Integer>();
    static{
        anltrQpMap.put(TSParser.KW_AND, SQLConstant.KW_AND);
        anltrQpMap.put(TSParser.KW_OR, SQLConstant.KW_OR);
        anltrQpMap.put(TSParser.KW_NOT, SQLConstant.KW_NOT);
        
        anltrQpMap.put(TSParser.EQUAL, SQLConstant.EQUAL);
        anltrQpMap.put(TSParser.NOTEQUAL, SQLConstant.NOTEQUAL);
        anltrQpMap.put(TSParser.LESSTHANOREQUALTO, SQLConstant.LESSTHANOREQUALTO);
        anltrQpMap.put(TSParser.LESSTHAN, SQLConstant.LESSTHAN);
        anltrQpMap.put(TSParser.LESSTHANOREQUALTO, SQLConstant.LESSTHANOREQUALTO);
        anltrQpMap.put(TSParser.LESSTHAN, SQLConstant.LESSTHAN);
        anltrQpMap.put(TSParser.GREATERTHANOREQUALTO, SQLConstant.GREATERTHANOREQUALTO);
        anltrQpMap.put(TSParser.GREATERTHAN, SQLConstant.GREATERTHAN);
        anltrQpMap.put(TSParser.EQUAL_NS, SQLConstant.EQUAL_NS);
        
        anltrQpMap.put(TSParser.TOK_SELECT, SQLConstant.TOK_SELECT);
        anltrQpMap.put(TSParser.TOK_FROM, SQLConstant.TOK_FROM);
        anltrQpMap.put(TSParser.TOK_WHERE, SQLConstant.TOK_WHERE);
        anltrQpMap.put(TSParser.TOK_QUERY, SQLConstant.TOK_QUERY);
    }
    
    public static int getTSTokenIntType(int anltrIntType) {
        if(!anltrQpMap.containsKey(anltrIntType))
            System.out.println(anltrIntType);
        return anltrQpMap.get(anltrIntType);
    }
    
}
