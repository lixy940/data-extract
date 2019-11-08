package com.lixy.dataextract.vo;
/**
 * 数据库访问连接对象
 * @Author: MR LIS
 * @Date: 16:44 2018/7/18
 */
public class DbConnInfo {
    /**
     * 数据库访问url
     */
    private String url;
    /**
     * 用户名
     */
    private String userName;
    /**
     * 密码
     */
    private String passWord;

    /**
     * 数据库驱动
     */
    private String dbDriver;

    public DbConnInfo(){

    }

    public DbConnInfo(String userName, String passWord) {
        this.userName = userName;
        this.passWord = passWord;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassWord() {
        return passWord;
    }

    public void setPassWord(String passWord) {
        this.passWord = passWord;
    }

    public String getDbDriver() {
        return dbDriver;
    }

    public void setDbDriver(String dbDriver) {
        this.dbDriver = dbDriver;
    }
}
