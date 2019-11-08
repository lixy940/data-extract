package com.lixy.dataextract.enums;

/**
 * @author yuanyang
 * @Description:基础异常类型
 * @date 2018/6/29 9:18
 */
public enum  BaseExceptionType implements ExceptionType{


    SUCCESS(200,"success"),
    PARAM_ERROR(414,"param is error"),
    SERVICE_ERROR(500,"service is error");

    BaseExceptionType(int code,String description){
        this.code = code;
        this.description = description;
    }

    /**
     * 异常码
     */
    private int code;

    /**
     * 异常描述
     */
    private String description;

    @Override
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}

