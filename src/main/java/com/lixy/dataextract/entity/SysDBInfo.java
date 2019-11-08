package com.lixy.dataextract.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * 数据库连接对象，对应数据库表sys_dbinfo
 */
public class SysDBInfo {

    private Integer dbinfoId;
    /**
     * 数据库名字
     */
    private String dbName;
    /**
     * 数据库类型
     */
    private String dbType;
    /**
     * 区域类型：离线：1  专题：2  其他数据连接：3  沙盘结果输出：10
     */
    private Integer areaType;
    /**
     * ip
     */
    private String dbIp;

    /**
     * 端口
     */
    private String dbPort;
    /**
     * 数据库服务名
     */
    private String dbServerName;
    /**
     * 其他字段，postgres中同一个库分为不同的模式
     */
    private String dbTableSchema;
    /**
     * postgresql库区分视图与表 r:表 v:视图
     */
    private String dbRelkind;
    /**
     * 用户名
     */
    private String dbUser;
    /**
     * 密码
     */
    private String dbPassword;
    /**
     * 状态
     */
    private Integer status;
    /**
     * 创建人
     */
    private String createPersonId;

    /**
     * 创建时间
     */
    private Date createTime;

    public Integer getDbinfoId() {
        return dbinfoId;
    }

    public void setDbinfoId(Integer dbinfoId) {
        this.dbinfoId = dbinfoId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName == null ? null : dbName.trim();
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType == null ? null : dbType.trim();
    }

    public String getDbIp() {
        return dbIp;
    }

    public void setDbIp(String dbIp) {
        this.dbIp = dbIp == null ? null : dbIp.trim();
    }

    public String getDbPort() {
        return dbPort;
    }

    public void setDbPort(String dbPort) {
        this.dbPort = dbPort == null ? null : dbPort.trim();
    }

    public String getDbServerName() {
        return dbServerName;
    }

    public void setDbServerName(String dbServerName) {
        this.dbServerName = dbServerName == null ? null : dbServerName.trim();
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword == null ? null : dbPassword.trim();
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getCreatePersonId() {
        return createPersonId;
    }

    public void setCreatePersonId(String createPersonId) {
        this.createPersonId = createPersonId == null ? null : createPersonId.trim();
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbTableSchema() {
        return dbTableSchema;
    }

    public void setDbTableSchema(String dbTableSchema) {
        this.dbTableSchema = dbTableSchema;
    }

    public String getDbRelkind() {
        return dbRelkind;
    }

    public void setDbRelkind(String dbRelkind) {
        this.dbRelkind = dbRelkind;
    }

    public Integer getAreaType() {
        return areaType;
    }

    public void setAreaType(Integer areaType) {
        this.areaType = areaType;
    }
}