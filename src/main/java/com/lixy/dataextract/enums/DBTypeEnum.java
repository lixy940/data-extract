package com.lixy.dataextract.enums;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据库类型 用于配置数据源连接时判断数据库类型
 * Created by ygs on 2017/12/22.
 */
public enum DBTypeEnum {
    DB_ORACLE("oracle"),
    DB_MYSQL("mysql"),
    DB_POSTGRESQL("postgresql"),
    DB_TIDB("tidb"),
    DB_H2("h2")

    ;

    private String dbName;

    DBTypeEnum(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public static List<String> getDbTypeList(){
        List<String> list = new ArrayList<>();
        for (DBTypeEnum value : DBTypeEnum.values()) {
            list.add(value.getDbName());
        }
        return list;
    }

}
