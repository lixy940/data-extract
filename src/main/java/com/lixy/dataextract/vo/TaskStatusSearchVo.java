package com.lixy.dataextract.vo;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public class TaskStatusSearchVo extends PageVo {

    private Integer dbId;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 任务处理状态
     */
    private Integer status;

    public String getTableName() {
        return tableName != null ? tableName.trim() : tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getDbId() {
        return dbId;
    }

    public void setDbId(Integer dbId) {
        this.dbId = dbId;
    }
}
