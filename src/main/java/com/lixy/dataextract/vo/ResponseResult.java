package com.lixy.dataextract.vo;

import com.lixy.dataextract.enums.BaseExceptionType;
import com.lixy.dataextract.enums.BizException;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

/**
 * @author YangGuisen
 * @version 1.0
 * @date 2017年6月20日
 */
@SuppressWarnings("serial")
@ApiModel("接口返回对象")
@JsonInclude(JsonInclude.Include.ALWAYS)
public class ResponseResult<T> implements Serializable {

    @ApiModelProperty("请求结果值")
    private T data;


    @ApiModelProperty("请求结果状态")
    private int status = BaseExceptionType.SUCCESS.getCode();

    @ApiModelProperty("请求结果消息描述")
    private String msg = BaseExceptionType.SUCCESS.getDescription();


    public ResponseResult() {

    }

    public ResponseResult(T data, String msg) {
        this.data = data;
        this.msg = msg;
    }


    public ResponseResult(T data) {
        this.data = data;
    }

    public ResponseResult(T data, int status, String msg) {
        this.data = data;
        this.status = status;
        this.msg = msg;
    }

    public ResponseResult(int status, String msg) {
        this.status = status;
        this.msg = msg;
    }

    public ResponseResult(BizException type){
        this(type.getCode(),type.getDescription());
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

}
