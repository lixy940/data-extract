package com.lixy.dataextract.dao;

import com.lixy.dataextract.entity.CommitTableRecord;
import com.lixy.dataextract.vo.CommitTableSearchVo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
@Repository
public interface CommitTableRecordMapper {

    @Select(" select count(0) from commit_table_record where db_id=#{dbId} and table_name=#{tableName}")
    int findCoutByDbIdAndTableName(@Param("dbId") int dbId, @Param("tableName") String tableName);

    int insert(CommitTableRecord commitTableRecord);

    CommitTableRecord selectByPrimaryKey(Integer commitId);

    void deleteByPrimaryKey(Integer commitId);

    int updateByPrimaryKey(CommitTableRecord commitTableRecord);

    List<CommitTableRecord> findPage(CommitTableSearchVo record);

    /**
     * 获取表总记录数
     * @param tableName
     * @return
     */
    int findCountTableName(@Param("tableName") String tableName);


    int selectCountByDbId(@Param("dbId") Integer dbId);
}
