package com.lixy.dataextract.dao;

import com.lixy.dataextract.entity.SysDBInfo;
import com.lixy.dataextract.vo.DbSearchVo;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SysDBInfoMapper {
    Integer deleteByPrimaryKey(Integer dbinfoId);

    Integer insert(SysDBInfo record);

    Integer insertSelective(SysDBInfo record);

    SysDBInfo selectByPrimaryKey(Integer dbinfoId);

    Integer updateByPrimaryKeySelective(SysDBInfo record);

    Integer updateByPrimaryKey(SysDBInfo record);

    /**
     * 获取所有db连接
     * @Author: MR LIS
     * @Date: 16:21 2018/7/30
     * @return
     */
    List<SysDBInfo> selectAll();

    /**
     * 支持通过关键字过滤查找
     * @param dbName 数据库名称
     * @param dbType 数据库类型
     * @param pageIndex
     * @param pageSize
     * @return
     */
    List<SysDBInfo> filterListByKeyword(@Param("dbType") String dbType,
                                        @Param("dbName") String dbName,
                                        @Param("pageIndex") Integer pageIndex,
                                        @Param("pageSize") Integer pageSize);

    /**
     * 统计结果数量
     * @param dbType
     * @param dbName
     * @return
     */
    Integer countWithKeyword(@Param("dbType") String dbType,
                             @Param("dbName") String dbName);

    /**
     * @Author: MR LIS
     * @Description:
     * @Date: 14:46 2018/5/31
     * @return
     */
    List<SysDBInfo> selectListByAreaType();

    /**
     * 根据服务连接进行筛选
     * @param dbName
     * @return
     */
    List<SysDBInfo> findListByKeyword(@Param("dbName") String dbName);

    /**
     * 获取数据库连接分页
     * @param record
     * @return
     */
    List<SysDBInfo> findDbPage(DbSearchVo record);
}