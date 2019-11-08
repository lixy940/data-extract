package com.lixy.dataextract.vo;

import java.io.Serializable;
import java.util.Date;

/**
 * Author：MR LIS，2019/10/22
 * Copyright(C) 2019 All rights reserved.
 */
public class HandlerTaskVo implements Serializable {

    /**
     * 已提交表id
     */
    private Integer commitId;
    /**
     * 定时周期表达式
     */
    private String cronExp;

    /**
     * 任务状态，0 未开始，1 进行中，2 已完成
     */
    private Integer taskStatus;
    /**
     * 任务类型 1 永久一次,2 周期执行，3 定时执行
     */
    private Integer taskType;

    /**
     * 任务最新操作时间
     */
    private Date lastHandleTime;

    /**
     *
     */
    private TaskDateTimeVo taskDateTimeVo;

    public String getCronExp() {
        return cronExp;
    }

    public void setCronExp(String cronExp) {
        this.cronExp = cronExp;
    }

    public Integer getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(Integer taskStatus) {
        this.taskStatus = taskStatus;
    }

    public Integer getTaskType() {
        return taskType;
    }

    public void setTaskType(Integer taskType) {
        this.taskType = taskType;
    }

    public Date getLastHandleTime() {
        return lastHandleTime;
    }

    public void setLastHandleTime(Date lastHandleTime) {
        this.lastHandleTime = lastHandleTime;
    }

    public TaskDateTimeVo getTaskDateTimeVo() {
        return taskDateTimeVo;
    }

    public void setTaskDateTimeVo(TaskDateTimeVo taskDateTimeVo) {
        this.taskDateTimeVo = taskDateTimeVo;
    }

    public Integer getCommitId() {
        return commitId;
    }

    public void setCommitId(Integer commitId) {
        this.commitId = commitId;
    }
}
