package com.lixy.dataextract.vo;

/**
 * Author：MR LIS，2019/10/22
 * Copyright(C) 2019 All rights reserved.
 */
public class DbSearchVo extends PageVo {

    /**
     * 数据库名称
     */
    private String dbName;

    /**
     * 数据库类型
     */
    private String dbType;

    public String getDbName() {
        return dbName != null ? dbName.trim() : dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }
}
