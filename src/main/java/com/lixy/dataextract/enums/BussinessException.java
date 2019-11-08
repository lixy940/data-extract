package com.lixy.dataextract.enums;

import org.apache.commons.lang3.ArrayUtils;

/**
 * @author yuanyang
 * @Description:pas业务异常定义
 * @date 2018/5/29 16:24
 */
public class BussinessException extends RuntimeException implements BizException{

    private Integer code;

    private String description;

    public BussinessException(String message) {
        super(message);
        this.description = message;
    }

    public BussinessException(ExceptionType type, String message) {
        super(message);
        this.description = message;
        this.code = type.getCode();
    }

    public BussinessException(ExceptionType type) {
        this(type,type.getDescription());
    }

    public BussinessException(ExceptionType type, Object... args) {
        this(type, ArrayUtils.isNotEmpty(args) ? String.format(type.getDescription(), args) : type.getDescription());
    }

    public BussinessException(int exceptionCode, String message) {
        super(message);
        this.code = exceptionCode;
        this.description = message;
    }

    @Override
    public int getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

}
