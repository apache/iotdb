package cn.edu.thu.tsfiledb.qp.exception.physical;

import cn.edu.thu.tsfile.timeseries.utils.StringContainer;

public class NamespacePlanException extends PhysicalPlanException {

    private static final long serialVersionUID = -2069701664732857589L;

    public NamespacePlanException(String msg){
        super(msg);
    }
    public NamespacePlanException(String msg, Exception exception) {
        super(msg);
        StringContainer sc = new StringContainer(",");
        sc.addTail("error message:", msg, "meet other exceptions:");
        sc.addTail("exception type", exception.getClass().toString(), "exception message",
                exception.getMessage());
        errMsg = sc.toString();
    }
}
