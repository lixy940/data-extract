package com.lixy.dataextract.vo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

/**
 * 绑定任务时间
 */
@ApiModel(value = "周期任务对象")
public class TaskDateTimeVo implements Serializable {

    @ApiModelProperty("月")
    private Integer months;

    @ApiModelProperty("日")
    private Integer days;

    @ApiModelProperty("时")
    private Integer hour;

    @ApiModelProperty("分")
    private Integer minute;

    /**
     * 时分 默认从0 日和月默认从1
     */
    @ApiModelProperty("时分 默认为0 从现在开始间隔， 日和月默认从1日")
    private Integer fromBeginNum;


    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public Integer getMinute() {
        return minute;
    }

    public void setMinute(Integer minute) {
        this.minute = minute;
    }

    public Integer getDays() {
        return days;
    }

    public void setDays(Integer days) {
        this.days = days;
    }

    public Integer getMonths() {
        return months;
    }

    public void setMonths(Integer months) {
        this.months = months;
    }

    public Integer getFromBeginNum() {
        return fromBeginNum;
    }

    public void setFromBeginNum(Integer fromBeginNum) {
        this.fromBeginNum = fromBeginNum;
    }
}
