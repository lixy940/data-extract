package com.lixy.dataextract.entity;

import com.lixy.dataextract.enums.ColumnTypeEnum;
import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public class CommitTableRecord {

    private Integer commitId;

    private Integer dbId;

    private String tableName;

    private String path;

    private String etlCol;

    private Integer dataType;

    private Date createTime;

    private String dbName;

    private String dataTypeName;

    public Integer getCommitId() {
        return commitId;
    }

    public void setCommitId(Integer commitId) {
        this.commitId = commitId;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getEtlCol() {
        return etlCol;
    }

    public void setEtlCol(String etlCol) {
        this.etlCol = etlCol;
    }

    public int getDataType() {
        return dataType;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Integer getDbId() {
        return dbId;
    }

    public void setDbId(Integer dbId) {
        this.dbId = dbId;
    }

    public void setDataType(Integer dataType) {
        this.dataType = dataType;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDataTypeName() {
        return ColumnTypeEnum.getName(this.dataType);
    }


}
