package cn.edu.thu.tsfiledb.qp.exec;

/**
 * condition has three type:time, freq(frequency) and ordinary value.
 * 
 * @author kangrong
 *
 */
public enum FilterType {
    TIME(0), FREQ(1), VALUE(2);
    private int typeCode;

    private FilterType(int i) {
        typeCode = i;
    }

    public int getTypeCode() {
        return typeCode;
    }

    public static FilterType valueOfStr(String v) {
        v = v.toLowerCase();
        switch (v) {
            case "time":
                return TIME;
            case "freq":
                return FREQ;
            default:
                return VALUE;
        }
    }
}
