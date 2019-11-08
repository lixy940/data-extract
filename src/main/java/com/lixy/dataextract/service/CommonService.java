package com.lixy.dataextract.service;

import com.lixy.dataextract.entity.SysDBInfo;
import com.lixy.dataextract.vo.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

/**
 * @Author: MR LIS
 * @Description:公共服务接口
 * @Date: Create in 14:45 2018/5/25
 * @Modified By:
 */
public interface CommonService {
    /**
     * @param dbId      数据库id
     * @param tableName 表名
     * @return
     * @Author: MR LIS
     * @Description:获取数据库表的字段名、注释、数据类型
     * @Date: 14:50 2018/5/25
     */
    List<ColumnInfoVO> getAllColumnInfo(Integer dbId, String tableName) ;

    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据dbId，tableName获取总记录数
     * @Date: 14:54 2018/5/25
     */
    int executePageTotalCount(Integer dbId, String tableName) ;

    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据dbId，tableName执行分页查询，不进行总记录数的查询
     * @Date: 14:54 2018/5/25
     */
    List<List<Object>> executePageQueryNotCount(Integer dbId, String tableName, Integer pageNum, Integer pageSize) ;

    /**
     * @Description: 根据dbId，tableName获取带条件总记录数
     * @return
     * @Author: MR LIS
     * @Date: 14:54 2018/5/25
     */
    int executePageTotalCountWithCondition(ConditionCountVo countVo) ;

    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据dbId，tableName执行带条件分页查询，不进行总记录数的查询
     * @Date: 14:54 2018/5/25
     */
    List<List<Object>> executePageQueryNotCountWithCondition(ConditionPageVo pageVo, String columnArr) ;
    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据dbId，tableName执行分页查询，不进行总记录数的查询,并返回指定列的数据
     * @Date: 14:54 2018/5/25
     */
    List<List<Object>> executePageQueryColumnRecord(Integer dbId, String tableName, String columnArr, Integer pageNum, Integer pageSize) ;

    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据dbId，tableName执行分页查询，不进行总记录数的查询,并返回指定列的数据+排序
     * @Date: 14:54 2018/5/25
     */
    List<List<Object>> executePageQueryColumnSortRecord(Integer dbId, String tableName, String columnArr, Integer pageNum, Integer pageSize, String sortField, String descType);
    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据库进行制定表的删除
     * @Date: 10:44 2018/6/1
     */
    void dropTable(Integer dbId, String tableName);

    /**
     * 判断指定表名在对应库或者模式中的个数
     * @param dbId
     * @param tableName
     * @return
     * @
     * @Author: MR LIS
     * @Date: 13:05 2018/9/3
     */
    int getIsTableExistCount(Integer dbId, String tableName);

    /**
     * 批量插入数据
     * @param dbInfo
     * @param sql
     * @param columnNum
     * @param list
     * @param batchOptNum
     */
    void batchInsert(DbConnInfo dbInfo, String sql, Integer columnNum, List<Map<Integer, Object>> list, Integer batchOptNum);

    void batchInsert(DbConnInfo dbInfo, String insertSql, List<List<Object>> datas, int columnSize, Integer batchNum);

    /**
     * 执行更新
     * @param dbInfo
     * @param sql
     */
    void executeJdbc(DbConnInfo dbInfo, String sql);

    /**
     * 获取所有数据
     *
     * @param dbInfo
     * @param tableName
     * @param limitCount 查询的记录数长度 如果为null，则为全部
     * @return
     */
     List<Map<String,Object>> getAllDatas(DbConnInfo dbInfo, String tableName, List<String> columnListList, Integer limitCount);

    /**
     * 获取所有列序号对应的value结果集合
     * @param dbInfo
     * @param tableName
     * @param limitCount 查询的记录数长度 如果为null，则为全部
     * @return
     */
     List<Map<Integer,Object>> getColumnIndexValueMaps(DbConnInfo dbInfo, String tableName, List<String> columnListList, Integer limitCount);

    /**
     * 组装插入sql
     * @param columnList 列名集合
     * @return
     */
    String assembleInsertSql(List<String> columnList, String tableName);

    /**
     * 关闭连接
     * @param conn
     * @param pstm
     */
    void closeConn(Connection conn, PreparedStatement pstm);

    /**
     * 关闭连接
     * @param conn
     * @param stmt
     * @param rs
     */
    void closeConn(Connection conn, PreparedStatement stmt, ResultSet rs);



    /**
     * 根据数据库连接属性，拼接数据库连接信息
     *
     * @param dbId 数据库连接id
     * @return
     * @Author: MR LIS
     * @Date: 16:58 2018/7/18
     */
    DbConnInfo setDbConnInfo(Integer dbId);
    /**
     * 组装sql连接
     * @param sysDBInfo
     * @return
     */
    DbConnInfo setDbConnInfo(SysDBInfo sysDBInfo);


    /**
     * 创建表
     *
     * @param dbConnInfo
     * @param sql
     */
    void createDbDDL(DbConnInfo dbConnInfo, String sql);
    /**
     * 获取表预览信息
     *
     * @param dbId
     * @param tableName
     * @return
     * @throws Exception
     */
    TableViewVo getColumnListBy(Integer dbId, String tableName);


    /**
     * 过滤筛选
     * @param dbTableInfos
     * @param pageQueryVo
     */
    SourceTablePageVo filterCondition(List<SourceDataInfoShowVO> dbTableInfos, SourceTableSearchVo pageQueryVo);

    /**
     * 测试数据库连接
     * @param dbInfo
     * @return
     * @throws ClassNotFoundException
     */
    Boolean getDataSourceStatus(DataSourceSaveVO dbInfo) ;
}
