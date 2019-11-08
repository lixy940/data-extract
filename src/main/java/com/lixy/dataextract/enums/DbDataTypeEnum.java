package com.lixy.dataextract.enums;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: MR LIS
 * @Description:数据库数据类型，比较宽泛的定义，便于mysql、oracle等的通用
 * @Date: Create in 11:05 2018/5/28
 * @Modified By:
 */
public enum DbDataTypeEnum {

    NUMBER("number","数字(整数)"),
    STRING("string","字符串"),
    FLOAT("float","浮点型数据(小数)"),
    DATE("date","日期、时间"),
    GEO("geo", "geo")
    ;

    private String type;
    /**
     * 中文名称
     */
    private String name;
     DbDataTypeEnum(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

   public static List<DataType> getDbTypeList(){
       List<DataType> dataTypes = new ArrayList<>();
       DbDataTypeEnum[] values = DbDataTypeEnum.values();
       for (DbDataTypeEnum value : values) {
           dataTypes.add(new DataType(value.getName(), value.getType()));
       }

       return dataTypes;
   }

   private static class DataType{
       /**
        * 中文名称
        */
       private String cname;
       /**
        * 英文名称
        */
       private String ename;

       public DataType(String cname, String ename) {
           this.cname = cname;
           this.ename = ename;
       }

       public String getCname() {
           return cname;
       }

       public void setCname(String cname) {
           this.cname = cname;
       }

       public String getEname() {
           return ename;
       }

       public void setEname(String ename) {
           this.ename = ename;
       }
   }
}
