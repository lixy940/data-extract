package com.lixy.dataextract.service;

import com.lixy.dataextract.vo.ColumnInfoVO;
import com.lixy.dataextract.vo.SourceTablePageVo;
import com.lixy.dataextract.vo.SourceTableSearchVo;

import java.util.List;

/**
 * Author：MR LIS，2019/10/28
 * Copyright(C) 2019 All rights reserved.
 */
public interface H2DbService {
    /**
     * 获取所有表
     * @param dbId
     * @return
     */
    SourceTablePageVo findTablesBySchema(SourceTableSearchVo pageQueryVo);

    /**
     * 获取指定表的总记录
     * @param dbId
     * @param tableName
     * @return
     */
    int findTotalCount(Integer dbId, String tableName);


    List<List<Object>> findRecords(Integer dbId, String tableName, Integer pageNo, Integer pageSize);

    /**
     * 获取所有列名
     * @param dbId
     * @param tableName
     * @return
     */
    List<ColumnInfoVO>  getAllColumnS(Integer dbId, String tableName);

}
