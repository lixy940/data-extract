package com.lixy.dataextract.vo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * Created by
 *
 * @author Zhangduoli
 * @date 2019-4-18
 */
@ApiModel("数据源保存对象")
public class DataSourceSaveVO {
    /**
     * 库id
     */
    @ApiModelProperty("库id")
    private Integer dbInfoId;
    /**
     * 数据库类型
     */
    @ApiModelProperty("数据库类型")
    private String dbType;

    /**
     * ip
     */
    @ApiModelProperty("ip")
    private String dbIp;

    /**
     * 端口
     */
    @ApiModelProperty("端口")
    private int dbPort;
    /**
     * 数据库服务名
     */
    @ApiModelProperty("数据库服务名")
    private String dbServerName;
    /**
     * 其他字段，postgres中同一个库分为不同的模式
     *
     */
    @ApiModelProperty("模式")
    private String dbTableSchema;

    /**
     * 用户名
     */
    @ApiModelProperty("用户名")
    private String dbUser;
    /**
     * 密码
     */
    @ApiModelProperty("密码")
    private String dbPassword;

    public String getDbType() {
        return dbType.toLowerCase();
    }

    public void setDbType(String dbType) {
        this.dbType = dbType.toLowerCase();
    }

    public String getDbIp() {
        return dbIp;
    }

    public void setDbIp(String dbIp) {
        this.dbIp = dbIp;
    }

    public int getDbPort() {
        return dbPort;
    }

    public void setDbPort(int dbPort) {
        this.dbPort = dbPort;
    }

    public Integer getDbInfoId() {
        return dbInfoId;
    }

    public void setDbInfoId(Integer dbInfoId) {
        this.dbInfoId = dbInfoId;
    }


    public String getDbServerName() {
        return dbServerName;
    }

    public void setDbServerName(String dbServerName) {
        this.dbServerName = dbServerName;
    }

    public String getDbTableSchema() {
        return dbTableSchema;
    }

    public void setDbTableSchema(String dbTableSchema) {
        this.dbTableSchema = dbTableSchema;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }
}
