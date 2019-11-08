package com.lixy.dataextract.vo;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public class TaskSearchVo extends PageVo {


    /**
     * 表名
     */
    private String tableName;
    /**
     * 数据库连接id
     */
    private String dbId;

    public String getTableName() {
        return tableName != null ? tableName.trim() : tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
    }
}
