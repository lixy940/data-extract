package com.lixy.dataextract.enums;

/**
 * @author yuanyang
 * @Description:业务异常类型顶层接口,推荐使用枚举来进行实现
 * @date 2018/5/29 16:34
 */
public interface ExceptionType {

    /**
     * 异常码
     */
    int getCode();

    /**
     * 异常描述
     */
    String getDescription();
}
