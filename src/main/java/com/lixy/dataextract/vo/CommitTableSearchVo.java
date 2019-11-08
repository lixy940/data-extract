package com.lixy.dataextract.vo;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public class CommitTableSearchVo extends PageVo {
    private Integer dbId;

    private String tableName;

    private String etlCol;

    private Integer dataType;

    public Integer getDbId() {
        return dbId;
    }

    public void setDbId(Integer dbId) {
        this.dbId = dbId;
    }

    public String getTableName() {
        return tableName != null ? tableName.trim() : tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEtlCol() {
        return etlCol;
    }

    public void setEtlCol(String etlCol) {
        this.etlCol = etlCol;
    }

    public Integer getDataType() {
        return dataType;
    }

    public void setDataType(Integer dataType) {
        this.dataType = dataType;
    }
}
