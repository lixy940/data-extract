package com.lixy.dataextract.vo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;

/**
 * Created by Guopengzhan on 2018/5/24.
 *
 */
@ApiModel("列信息")
public class ColumnInfoVO {

    /**
     * 列的英文名字
     */
    @ApiModelProperty("选中列英文名")
    private String columnEname;

    /**
     *列中文名
     */
    @ApiModelProperty("选中列中文名")
    @NotBlank
    private String columnCname;

    /**
     *列属性
     */
    @ApiModelProperty("列数据类型(number,string,float,date)")
    private String columnType;

    public ColumnInfoVO() {
    }

    public ColumnInfoVO(String columnEname, String columnCname, String columnType) {
        this.columnEname = columnEname;
        this.columnCname = columnCname;
        this.columnType = columnType;
    }

    public String getColumnEname() {
        return columnEname;
    }

    public void setColumnEname(String columnEname) {
        this.columnEname = columnEname;
    }

    public String getColumnCname() {
        return columnCname;
    }

    public void setColumnCname(String columnCname) {
        this.columnCname = columnCname;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }
}
