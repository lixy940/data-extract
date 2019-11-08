package com.lixy.dataextract.enums;

/**
 * @Author: MR LIS
 * @Description:驱动名称枚举
 * @Date: Create in 17:37 2018/5/24
 * @Modified By:
 */
public enum DriverNameEnum {

    DRIVER_MYSQL("com.mysql.cj.jdbc.Driver"),
    DRIVER_ORACLE("oracle.jdbc.driver.OracleDriver"),
    DRIVER_POSTGRES("org.postgresql.Driver"),
    DRIVER_H2("org.h2.Driver"),

    ;

    private String driverName;

    DriverNameEnum(String driverName) {
        this.driverName = driverName;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }
}
