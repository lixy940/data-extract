package com.lixy.dataextract.entity;

import java.util.Date;

public class HandlerTask {
    /**
     * 任务id
     */
    private Integer taskId;
    /**
     * 已提交表id
     */
    private Integer commitId;

    /**
     * 最新处理时间
     */
    private Date lastHandleTime;
    /**
     * 定时周期表达式
     */
    private String cronExp;

    /**
     * 任务周期
     */
    private String taskPeriod;

    /**
     * 任务状态
     */
    private int taskStatus;
    /**
     * 任务类型
     */
    private int taskType;

    private Date createTime;

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public Integer getCommitId() {
        return commitId;
    }

    public void setCommitId(Integer commitId) {
        this.commitId = commitId;
    }

    public Date getLastHandleTime() {
        return lastHandleTime;
    }

    public void setLastHandleTime(Date lastHandleTime) {
        this.lastHandleTime = lastHandleTime;
    }

    public String getCronExp() {
        return cronExp;
    }

    public void setCronExp(String cronExp) {
        this.cronExp = cronExp == null ? null : cronExp.trim();
    }

    public int getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(int taskStatus) {
        this.taskStatus = taskStatus;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public int getTaskType() {
        return taskType;
    }

    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    public String getTaskPeriod() {
        return taskPeriod;
    }

    public void setTaskPeriod(String taskPeriod) {
        this.taskPeriod = taskPeriod;
    }
}