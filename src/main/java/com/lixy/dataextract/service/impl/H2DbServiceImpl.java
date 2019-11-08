package com.lixy.dataextract.service.impl;

import com.lixy.dataextract.dao.SysDBInfoMapper;
import com.lixy.dataextract.entity.SysDBInfo;
import com.lixy.dataextract.enums.BussinessException;
import com.lixy.dataextract.enums.DriverNameEnum;
import com.lixy.dataextract.service.CommonService;
import com.lixy.dataextract.service.H2DbService;
import com.lixy.dataextract.utils.RegexUtils;
import com.lixy.dataextract.vo.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Author：MR LIS，2019/10/28
 * Copyright(C) 2019 All rights reserved.
 */
@Service
public class H2DbServiceImpl implements H2DbService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SysDBInfoMapper sysDBInfoMapper;

    @Autowired
    private CommonService commonService;


    @Override
    public SourceTablePageVo findTablesBySchema(SourceTableSearchVo pageQueryVo) {
        SysDBInfo sysDBInfo = sysDBInfoMapper.selectByPrimaryKey(pageQueryVo.getDbId());
        String sql = SqlAssembleFactory.buildSqlForTableS(sysDBInfo.getDbTableSchema());
        List<SourceDataInfoShowVO> dbTableInfos = getDbTableInfos(sysDBInfo, sql);
        return commonService.filterCondition(dbTableInfos,pageQueryVo);
    }

    @Override
    public int findTotalCount(Integer dbId, String tableName) {
        SysDBInfo sysDBInfo = sysDBInfoMapper.selectByPrimaryKey(dbId);
        String sql = SqlAssembleFactory.buildSqlForCount(sysDBInfo, tableName);
        return queryPageTotalCount(sysDBInfo, sql);
    }

    @Override
    public List<List<Object>> findRecords(Integer dbId, String tableName, Integer pageNo, Integer pageSize) {
        SysDBInfo sysDBInfo = sysDBInfoMapper.selectByPrimaryKey(dbId);
        List<ColumnInfoVO> allColumnS = getAllColumnS(dbId, tableName);
        List<String> collect = allColumnS.stream().map(o -> o.getColumnEname()).collect(Collectors.toList());
        String columnArr = StringUtils.join(collect, ",");
        String sql = SqlAssembleFactory.buildSqlForRecord(sysDBInfo, tableName,columnArr, pageNo, pageSize);
        return executePageRecord(sysDBInfo, sql);
    }

    @Override
    public List<ColumnInfoVO> getAllColumnS(Integer dbId, String tableName) {
        SysDBInfo sysDBInfo = sysDBInfoMapper.selectByPrimaryKey(dbId);
        String sql = SqlAssembleFactory.buildSqlForAllColumnS(sysDBInfo, tableName);
        return getAllColumnInfo(sysDBInfo, sql);
    }


    private List<SourceDataInfoShowVO> getDbTableInfos(SysDBInfo sysDBInfo, String sql) {
        List<SourceDataInfoShowVO> showVOs = new ArrayList<>();
        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        SourceDataInfoShowVO showVO = null;
        try {
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                showVO = new SourceDataInfoShowVO();
                SourceDataInfoVO sourceVO = new SourceDataInfoVO(sysDBInfo.getDbinfoId(), rs.getString(ColumnFieldEnum.TABLE_NAME.name), null);
                showVO.setCount(rs.getLong(ColumnFieldEnum.TABLE_ROWNUM.name));
                showVO.setSourceDataInfoVO(sourceVO);
                showVOs.add(showVO);
            }

        } catch (Exception e) {
            logger.error("getDbTableInfos>>> {}:{}/{},异常:{}", sysDBInfo.getDbIp(), sysDBInfo.getDbPort(), sysDBInfo.getDbServerName(), e.getMessage());
            e.printStackTrace();
            throw new BussinessException("数据库连接无效，请检查连接配置");
        } finally {
            closeConn(conn, stmt, rs);
        }


        return showVOs;


    }


    private  int queryPageTotalCount(SysDBInfo sysDBInfo, String countSql) {
        //查询总记录数
        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int count = 0;
        try {
            stmt = conn.prepareStatement(countSql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("queryPageTotalCount>>> {}:{}/{},异常:{}", sysDBInfo.getDbIp(), sysDBInfo.getDbPort(), sysDBInfo.getDbServerName(), e.getMessage());
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, rs);
        }

        return count;
    }


    private  List<List<Object>> executePageRecord(SysDBInfo sysDBInfo, String querySql) {
        List<List<Object>> listList = new ArrayList<>();
        //查询总记录数
        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {

            stmt = conn.prepareStatement(querySql);
            rs = stmt.executeQuery();
            ResultSetMetaData data = rs.getMetaData();
            //oracle会多带一列行数回来
            int rowNum =  data.getColumnCount();
            while (rs.next()) {
                List<Object> objectList = new ArrayList<>();
                for (int i = 1; i <= rowNum; i++) {
                    Object o = rs.getObject(i);
                    //不为null,且2014-01-01 15:05:29.0格式进行转换
                    if (!Objects.isNull(o)) {
                        String s = String.valueOf(o);
                        //判断是否为2014-01-01 15:05:29.0格式的时间
                        if (RegexUtils.validateTimestamp(s)) {
                            objectList.add(s.substring(0, s.indexOf(".")));
                            continue;
                        }

                    }
                    objectList.add(o);
                }
                listList.add(objectList);
            }
        } catch (SQLException e) {
            logger.error("executePageRecord>>> {}:{}/{},异常:{}", sysDBInfo.getDbIp(), sysDBInfo.getDbPort(), sysDBInfo.getDbServerName(), e.getMessage());
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, rs);
        }
        return listList;

    }

    private List<ColumnInfoVO> getAllColumnInfo(SysDBInfo sysDBInfo, String sql) {
        List<ColumnInfoVO> voList = new ArrayList<>();
        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            ColumnInfoVO infoVO = null;
            while (rs.next()) {
                infoVO = new ColumnInfoVO(rs.getString(ColumnFieldEnum.COLUMN_ENAME.name), rs.getString(ColumnFieldEnum.COLUMN_CNAME.name) == null ? rs.getString(ColumnFieldEnum.COLUMN_CNAME.name) : rs.getString(ColumnFieldEnum.COLUMN_CNAME.name), null);
                voList.add(infoVO);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, rs);
        }

        return voList;
    }


    private  synchronized Connection getConnection(SysDBInfo sysDBInfo) {
        Connection conn = null;
        try {
            Class.forName(DriverNameEnum.DRIVER_H2.getDriverName());
            String url = "jdbc:h2:tcp://" + sysDBInfo.getDbIp()  + "/" + sysDBInfo.getDbServerName();
            conn = DriverManager.getConnection(url, sysDBInfo.getDbUser(), sysDBInfo.getDbPassword());
        } catch (Exception e) {
            logger.error("数据库连接异常：" + (sysDBInfo.getDbIp() + "/" + sysDBInfo.getDbServerName()));
        }
        return conn;
    }


    private synchronized void closeConn(Connection conn, PreparedStatement stmt, ResultSet rs) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static class SqlAssembleFactory {

        public static String buildSqlForTableS(String schema) {
            return "select table_name,row_count_estimate as table_row_num from information_schema.tables  where table_schema='" + schema+"'";
        }

        public static String buildSqlForCount(SysDBInfo sysDBInfo, String tableName) {
            return "select  count(*) from " + sysDBInfo.getDbTableSchema() + "." + tableName;
        }


        public static String buildSqlForRecord(SysDBInfo sysDBInfo, String tableName,String columnArr, Integer pageNo, Integer pageSize) {
            int start = (pageNo - 1) * pageSize + 1;
            int end = (pageNo) * pageSize;
            return "select " +columnArr +" from (select  rownum as r,a.* from " + sysDBInfo.getDbTableSchema() + "." + tableName + " a order by 1) t where t.r >= " + start + " and t.r <= " + end;
        }

        public static String buildSqlForAllColumnS(SysDBInfo sysDBInfo, String tableName) {
            return "select column_name as column_ename,remarks as column_cname  from  information_schema.columns  where table_schema='" + sysDBInfo.getDbTableSchema() + "' and table_name='" + tableName + "'";
        }
    }


    private enum ColumnFieldEnum {

        TABLE_NAME("table_name"),
        TABLE_ROWNUM("table_row_num"),
        COLUMN_ENAME("column_ename"),
        COLUMN_CNAME("column_cname"),

        ;

        ColumnFieldEnum(String name) {
            this.name = name;
        }

        private String name;

    }
}
