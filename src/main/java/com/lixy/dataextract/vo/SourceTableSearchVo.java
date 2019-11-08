package com.lixy.dataextract.vo;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public class SourceTableSearchVo extends PageVo {

    private int dbId;

    private String tableName;

    public int getDbId() {
        return dbId;
    }

    public void setDbId(int dbId) {
        this.dbId = dbId;
    }

    public String getTableName() {
        return tableName != null ? tableName.trim() : tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
