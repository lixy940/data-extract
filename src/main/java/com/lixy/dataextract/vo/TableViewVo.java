package com.lixy.dataextract.vo;

import java.util.List;

/**
 * @author LIS
 * @date 2018/12/2116:44
 */
public class TableViewVo {
    /**
     * 列名集合
     */
    private  List<ColumnInfoVO> columnInfoVOList;
    /**
     * 数据信息
     */
    private List<List<Object>> dataList;

    public List<ColumnInfoVO> getColumnInfoVOList() {
        return columnInfoVOList;
    }

    public void setColumnInfoVOList(List<ColumnInfoVO> columnInfoVOList) {
        this.columnInfoVOList = columnInfoVOList;
    }

    public List<List<Object>> getDataList() {
        return dataList;
    }

    public void setDataList(List<List<Object>> dataList) {
        this.dataList = dataList;
    }
}
