package cn.edu.tsinghua.tsfile.read.filter.factory;


public enum FilterType {
    VALUE_FILTER("value"), TIME_FILTER("time");

    private String name;
    FilterType(String name){
        this.name = name;
    }

    @Override
    public String toString(){
        return name;
    }

}
