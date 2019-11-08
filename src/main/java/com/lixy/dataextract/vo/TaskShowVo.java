package com.lixy.dataextract.vo;

import com.lixy.dataextract.enums.TaskStatusEnum;
import com.lixy.dataextract.enums.TaskTypeEnum;
import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public class TaskShowVo {
    /**
     * 任务id
     */
    private Integer taskId;
    /**
     * 表名
     */
    private String tableName;

    /**
     * 数据库连接名
     */
    private String dbName;

    /**
     * 任务状态
     */
    private int taskStatus;

    /**
     * 任务周期
     */
    private String taskPeriod;

    /**
     * 任务类型
     */
    private int taskType;

    private String taskStatusName;
    private String taskTypeName;


    /**
     * 最新处理时间
     */
    private Date lastHandleTime;

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public int getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(int taskStatus) {
        this.taskStatus = taskStatus;
    }

    public int getTaskType() {
        return taskType;
    }

    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    public Date getLastHandleTime() {
        return lastHandleTime;
    }

    public void setLastHandleTime(Date lastHandleTime) {
        this.lastHandleTime = lastHandleTime;
    }

    public String getTaskStatusName() {
        return TaskStatusEnum.getName(taskStatus);
    }

    public String getTaskTypeName() {
        return TaskTypeEnum.getName(taskType);
    }

    public String getTaskPeriod() {
        return taskPeriod;
    }

    public void setTaskPeriod(String taskPeriod) {
        this.taskPeriod = taskPeriod;
    }
}
