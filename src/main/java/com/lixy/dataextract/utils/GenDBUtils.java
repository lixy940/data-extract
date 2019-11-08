package com.lixy.dataextract.utils;

import com.lixy.dataextract.entity.SysDBInfo;
import com.lixy.dataextract.enums.BussinessException;
import com.lixy.dataextract.enums.DBTypeEnum;
import com.lixy.dataextract.enums.DbDataTypeEnum;
import com.lixy.dataextract.enums.DriverNameEnum;
import com.lixy.dataextract.vo.ColumnInfoVO;
import com.lixy.dataextract.vo.ConditionVo;
import com.lixy.dataextract.vo.SourceDataInfoShowVO;
import com.lixy.dataextract.vo.SourceDataInfoVO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: MR LIS
 * @Description:JDBC连接工具 mysql与tidb原理差不多，驱动一样,字段属性基本一致
 * @Date: Create in 17:41 2018/5/24
 * @Modified By:
 */
public class GenDBUtils {
    private final static Logger logger = LoggerFactory.getLogger(GenDBUtils.class);
    /**
     * MYSQL前缀
     */
    public static String MYSQL_PREFIX = "jdbc:mysql://";
    /**
     * MYSQL后缀
     */
    public static String MYSQL_SUFFIX = "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    /**
     * ORACLE前缀
     */
    public static String ORACLE_PREFIX = "jdbc:oracle:thin:@//";
    /**
     * postgres前缀
     */
    public static String POSTGRES_PREFIX = "jdbc:postgresql://";

    /**
     * mysql查询列信息语句前缀
     */
    private static String COLUMN_MYSQL_PREFIX = "show full columns from ";
    /**
     * oracle 查询列信息语句前缀
     */
//    private static String COLUMN_ORACLE_PREFIX = "select a.COLUMN_NAME,a.DATA_TYPE,b.COMMENTS from user_tab_columns a LEFT JOIN user_col_comments b ON a.table_name=b.table_name AND a.COLUMN_NAME=b.COLUMN_NAME where a.table_name=";
    private static String COLUMN_ORACLE_PREFIX = "select a.COLUMN_NAME,a.DATA_TYPE,b.COMMENTS from all_tab_cols a LEFT JOIN all_col_comments b ON " +
            "a.OWNER=b.OWNER and a.TABLE_NAME=b.TABLE_NAME AND a.COLUMN_NAME=b.COLUMN_NAME where a.HIDDEN_COLUMN = 'NO' and a.owner=";
    /**
     * postgres 查询列信息语句前缀,同一个库的不同模式下不要有重复的数据库名
     */
    private static String COLUMN_POSTGRES_PREFIX = "SELECT\n" +
            "\tC.relname,\n" +
            "\tcol_description (A.attrelid, A.attnum) AS description,\n" +
            "\tformat_type (A.atttypid, A.atttypmod) AS data_type,\n" +
            "\tA.attname AS column_name\n" +
            "FROM\n" +
            "\t\n" +
            "  pg_class AS C,\n" +
            "\tpg_attribute AS A,\n" +
            "\tpg_namespace AS B\n" +
            "WHERE A.attrelid = C.oid\n" +
            "AND C.relnamespace = B.oid\n" +
            "AND A.attnum > 0\n" +
            "AND A.attname NOT LIKE '%pg.dropped%'\n" +
            "AND C.relname = ";

    /**
     * mysql字段属性、注释、数据类型
     */
    private static String MYSQL_COLUMN_NAME = "Field";
    private static String MYSQL_COLUMN_COMMENT = "Comment";
    private static String MYSQL_COLUMN_TYPE = "Type";
    /**
     * oracle字段属性、注释、数据类型
     */
    private static String ORACLE_COLUMN_NAME = "COLUMN_NAME";
    private static String ORACLE_COLUMN_COMMENT = "COMMENTS";
    private static String ORACLE_COLUMN_TYPE = "DATA_TYPE";
    /**
     * postgres字段属性、注释、数据类型
     */
    private static String POSTGRES_COLUMN_NAME = "column_name";
    private static String POSTGRES_COLUMN_COMMENT = "description";
    private static String POSTGRES_COLUMN_TYPE = "data_type";


    /**
     * 分页查询时的总记录sql和分页查询sql
     */
    public static String PAGE_COUNT_SQL = "countSql";
    public static String PAGE_QUERY_SQL = "querySql";

    /**
     * mysql库对应库对应的所有表sql,mysql针对库名
     */
    private static String TABLE_MYSQL_PREFIX = "select table_name, table_comment,table_rows as row_num from information_schema.tables where table_schema = ";
    /**
     * mysql库对应库对应的所有表记录数求和
     */
    private static String ROW_COUNT_MYSQL_PREFIX = "select sum(table_rows) as total_count from information_schema.tables where table_schema = ";

    /**
     * mysql库对应库对应的所有表个数
     */
    private static String TABLE_COUNT_MYSQL_PREFIX = "select count(TABLE_NAME) as count from  information_schema.tables where table_schema=  ";
    /**
     * mysql判断对应表的个数
     */
    private static String TABLE_IS_EXIST_MYSQL_PREFIX = "SELECT count(0) as total_count FROM information_schema.TABLES WHERE table_name = ";
    /**
     * oracle、库对应库对应的所有表sql，oracle也是后面跟库名
     */
//    private static String TABLE_ORACLE_PREFIX = "select t.table_name as table_name,t.comments as table_comment,d.num_rows as row_num  from user_tab_comments t left join dba_tables d on t.table_name=d.table_name and d.owner= ";
    private static String TABLE_ORACLE_PREFIX = "select d.table_name as table_name,d.comments as table_comment,t.num_rows as row_num  from all_tables t left join all_tab_comments d on (t.table_name=d.table_name and t.owner=d.owner) where d.owner= ";
    /**
     * oracle、库对应库对应的所有表记录数求和
     */
    private static String ROW_COUNT_ORACLE_PREFIX = "select sum(num_rows) as total_count from all_tables where owner= ";
    /**
     * oracle、库对应库对应的所有表个数
     */
    private static String TABLE_COUNT_ORACLE_PREFIX = "select count(TABLE_NAME) as count from all_tables where owner= ";
    /**
     * oracle 判断指定模式下对应表的个数
     */
    private static String TABLE_IS_EXIST_ORACLE_PREFIX = "select count(0) as total_count from all_tables where owner = ";

    /**
     * postgres库对应库对应的所有表sql，postgres后面跟模式名称，连接时已经确定了是哪个库
     */
    private static String TABLE_POSTGRES_PREFIX = "SELECT\n" +
            "\tr.relname AS TABLE_NAME,\n" +
            "\t(\n" +
            "\t\tSELECT\n" +
            "\t\t\tobj_description (\n" +
            "\t\t\t\t(n.nspname || '.' || r.relname) :: regclass,\n" +
            "\t\t\t\t'pg_class'\n" +
            "\t\t\t)\n" +
            "\t\tLIMIT 1\n" +
            "\t) AS table_comment ,\n" +
            "\tr.reltuples AS row_num\n" +
            "FROM\n" +
            "\tpg_class r\n" +
            "JOIN pg_namespace n ON r.relnamespace = n.oid\n" +
            "WHERE\n" +
            "\t n.nspname = ";
    /**
     * postgres库对应模式下 对应的所有表记录数求和
     */
    private static String ROW_COUNT_POSTGRES_PREFIX = "SELECT\n" +
            "SUM(r.reltuples) as total_count\n" +
            "FROM pg_class r\n" +
            "JOIN pg_namespace n ON r.relnamespace = n.oid\n" +
            "WHERE\n" +
            " n.nspname = ";

    /**
     * postgres库对应库对应的所有表个数
     */
    private static String TABLE_COUNT_POSTGRES_PREFIX = "select count(tablename) as count from pg_tables where schemaname= ";

    /**
     * postgrep 判断指定模式下对应表的个数
     */
    private static String TABLE_IS_EXIST_POSTGRES_PREFIX = "SELECT \n" +
            "count(0) as total_count\n" +
            "FROM pg_class r\n" +
            "JOIN pg_namespace n ON r.relnamespace = n.oid\n" +
            "WHERE\n" +
            " n.nspname =";

    /**
     * 表名
     */
    private static String TABLE_NAME = "table_name";
    /**
     * 表备注
     */
    private static String TABLE_COMMENT = "table_comment";
    /**
     * 表总记录数
     */
    private static String TABLE_ROWNUM = "row_num";

    /**
     * 对应每个库的总记录数
     */
    private static String ROW_DB_TOTAL_COUNT = "total_count";

    /**
     * 对应每个库的表个数
     */
    private static String TABLE_DB_TOTAL_COUNT = "count";

    /**
     * 删表sql
     */
    private static String DROP_TABLE_SQL = "DROP TABLE ";

    /**
     * 指定库或者模式下的指定表的个数
     */
    private static String TABLE_TOTAL_COUNT = "total_count";


    /**
     * @param sysDBInfo 数据库连接配置对象
     * @param tableName 表名
     * @return
     * @Author: MR LIS
     * @Description:获取数据库表的字段名、注释、数据类型
     * @Date: 17:45 2018/5/24
     */
    public static List<ColumnInfoVO> getAllColumnInfo(SysDBInfo sysDBInfo, String tableName) {
        List<ColumnInfoVO> voList = new ArrayList<>();
        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(getColumnPropertySQL(sysDBInfo, tableName));
            rs = stmt.executeQuery();
            ColumnInfoVO infoVO = null;
            while (rs.next()) {
                if (DBTypeEnum.DB_MYSQL.getDbName().equals(sysDBInfo.getDbType()) || DBTypeEnum.DB_TIDB.getDbName().equals(sysDBInfo.getDbType())) {
                    infoVO = new ColumnInfoVO(rs.getString(MYSQL_COLUMN_NAME), rs.getString(MYSQL_COLUMN_COMMENT) == null ? rs.getString(MYSQL_COLUMN_NAME) : rs.getString(MYSQL_COLUMN_COMMENT), convertDataType(rs.getString(MYSQL_COLUMN_TYPE)));
                } else if (DBTypeEnum.DB_ORACLE.getDbName().equals(sysDBInfo.getDbType())) {
                    infoVO = new ColumnInfoVO(rs.getString(ORACLE_COLUMN_NAME), rs.getString(ORACLE_COLUMN_COMMENT) == null ? rs.getString(ORACLE_COLUMN_NAME) : rs.getString(ORACLE_COLUMN_COMMENT), convertDataType(rs.getString(ORACLE_COLUMN_TYPE)));
                } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equals(sysDBInfo.getDbType())) {
                    infoVO = new ColumnInfoVO(rs.getString(POSTGRES_COLUMN_NAME), rs.getString(POSTGRES_COLUMN_COMMENT) == null ? rs.getString(POSTGRES_COLUMN_NAME) : rs.getString(POSTGRES_COLUMN_COMMENT), convertDataType(rs.getString(POSTGRES_COLUMN_TYPE)));
                }
                voList.add(infoVO);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, rs);
        }

        return voList;
    }


    /**
     * 最大连接连接数量
     */
    private static volatile AtomicInteger activeSize = new AtomicInteger(30);

    /**
     * 记录连接被创建个数
     */
    private static volatile AtomicInteger createCounter = new AtomicInteger(0);

    /**
     * 获取连接对象
     *
     * @param sysDBInfo
     * @return
     */
    public synchronized static Connection getConnection(SysDBInfo sysDBInfo) {
        Connection conn = null;
        //判断当前被创建的连接个数是否大于等于最大数量
        while (createCounter.get() >= activeSize.get()) {
            try {
                GenDBUtils.class.wait(3000);
                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            if (DBTypeEnum.DB_MYSQL.getDbName().equals(sysDBInfo.getDbType()) || DBTypeEnum.DB_TIDB.getDbName().equals(sysDBInfo.getDbType())) {
                Class.forName(DriverNameEnum.DRIVER_MYSQL.getDriverName());
                String url = MYSQL_PREFIX + sysDBInfo.getDbIp() + ":" + sysDBInfo.getDbPort() + "/" + sysDBInfo.getDbServerName() + MYSQL_SUFFIX;
                conn = DriverManager.getConnection(url, sysDBInfo.getDbUser(), sysDBInfo.getDbPassword());
            } else if (DBTypeEnum.DB_ORACLE.getDbName().equals(sysDBInfo.getDbType())) {
                Class.forName(DriverNameEnum.DRIVER_ORACLE.getDriverName());
                String url = ORACLE_PREFIX + sysDBInfo.getDbIp() + ":" + sysDBInfo.getDbPort() + "/" + sysDBInfo.getDbServerName();
                conn = DriverManager.getConnection(url, sysDBInfo.getDbUser(), sysDBInfo.getDbPassword());
            } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equals(sysDBInfo.getDbType())) {
                Class.forName(DriverNameEnum.DRIVER_POSTGRES.getDriverName());
                String url = POSTGRES_PREFIX + sysDBInfo.getDbIp() + ":" + sysDBInfo.getDbPort() + "/" + sysDBInfo.getDbServerName();
                conn = DriverManager.getConnection(url, sysDBInfo.getDbUser(), sysDBInfo.getDbPassword());
            }
            /**
             * 连接数增加1
             */
            createCounter.incrementAndGet();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据数据库连接+库名 判断表的个数
     * @Date: 9:43 2018/5/30
     */
    public static int getIsTableExistCount(SysDBInfo sysDBInfo, String tableName) {

        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int totalCount = 0;
        try {
            stmt = conn.prepareStatement(getIsTableExistSQL(sysDBInfo, tableName));
            rs = stmt.executeQuery();
            while (rs.next()) {
                totalCount = rs.getInt(TABLE_TOTAL_COUNT);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, rs);
        }

        return totalCount;

    }

    /**
     * 获取列的属性信息的sql拼接
     *
     * @param sysDBInfo
     * @param tableName
     * @return
     */
    private static String getColumnPropertySQL(SysDBInfo sysDBInfo, String tableName) {
        String sql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equals(sysDBInfo.getDbType()) || DBTypeEnum.DB_TIDB.getDbName().equals(sysDBInfo.getDbType())) {
            sql = COLUMN_MYSQL_PREFIX + tableName;
        } else if (DBTypeEnum.DB_ORACLE.getDbName().equals(sysDBInfo.getDbType())) {
            sql = COLUMN_ORACLE_PREFIX + "'" + sysDBInfo.getDbTableSchema() + "'"+" and a.table_name='"+tableName+"'";
        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equals(sysDBInfo.getDbType())) {
            sql = COLUMN_POSTGRES_PREFIX + "'" + tableName + "'" + " AND B.nspname = '" + sysDBInfo.getDbTableSchema() + "'" + " AND C.relkind = '" + sysDBInfo.getDbRelkind() + "'";
        }
        return sql;
    }

    /**
     * 拼接判断表是否存在
     *
     * @return
     */
    private static String getIsTableExistSQL(SysDBInfo sysDBInfo, String tableName) {
        String sql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equals(sysDBInfo.getDbType()) || DBTypeEnum.DB_TIDB.getDbName().equals(sysDBInfo.getDbType())) {
            sql = TABLE_IS_EXIST_MYSQL_PREFIX + "'" + tableName + "'";
        } else if (DBTypeEnum.DB_ORACLE.getDbName().equals(sysDBInfo.getDbType())) {
            sql = TABLE_IS_EXIST_ORACLE_PREFIX + "'" + sysDBInfo.getDbTableSchema() + "'" + " and table_name='" + tableName + "'";
        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equals(sysDBInfo.getDbType())) {
            String newTableName = tableName.toLowerCase();
            sql = TABLE_IS_EXIST_POSTGRES_PREFIX + "'" + sysDBInfo.getDbTableSchema() + "'" + " AND r.relname = '" + newTableName + "'" + " AND r.relkind = '" + sysDBInfo.getDbRelkind() + "'";
        }
        return sql;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 查询分页总记录数
     * @Date: 10:05 2018/5/25
     */
    public static int executePageTotalCount(SysDBInfo sysDBInfo, String tableName) {
        if (Objects.isNull(sysDBInfo)) {
            return 0;
        }
        return queryPageTotalCount(sysDBInfo, pagingCountSql(sysDBInfo.getDbType(), tableName, sysDBInfo.getDbTableSchema()));
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 查询分页总记录数
     * @Date: 10:05 2018/12/14
     */
    public static int executePageTotalCountWithCondition(SysDBInfo sysDBInfo, String tableName, List<ConditionVo> conditionVos) {
        return queryPageTotalCount(sysDBInfo, pagingCountSqlWithCondition(sysDBInfo.getDbType(), tableName, sysDBInfo.getDbTableSchema(), conditionVos));
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 查询总记录数
     * @Date: 10:05 2018/5/25
     */
    public static int queryPageTotalCount(SysDBInfo sysDBInfo, String countSql) {
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

    /**
     * @return
     * @Author: MR LIS
     * @Description: 查询分页记录结果, 不含总记录数
     * @Date: 10:05 2018/5/25
     */
    public static List<List<Object>> executePage(SysDBInfo sysDBInfo, String tableName, int pageSize, int start, int end) {

        return executePageRecord(sysDBInfo, pagingSql(sysDBInfo.getDbType(), tableName, sysDBInfo.getDbTableSchema(), pageSize, start, end));
    }

    public static List<List<Object>> executePageSpecial(SysDBInfo sysDBInfo, String columnArr, String tableName, int pageSize, int start, int end) {
        return executePageRecord(sysDBInfo, pagingSqlSpecial(sysDBInfo.getDbType(), tableName, columnArr, sysDBInfo.getDbTableSchema(), pageSize, start, end));
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 查询带条件分页记录结果, 不含总记录数
     * @Date: 10:05 2018/12/14
     */
    public static List<List<Object>> executePageWithCondition(SysDBInfo sysDBInfo, String tableName, String columnArr, List<ConditionVo> conditionVos, int pageSize, int start, int end) {
        return executePageRecord(sysDBInfo, pagingSqlWithCondition(sysDBInfo.getDbType(), tableName, columnArr, conditionVos, sysDBInfo.getDbTableSchema(), pageSize, start, end));
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 查询分页记录结果, 不含总记录数
     * @Date: 10:05 2018/5/25
     */
    public static List<List<Object>> executePage(SysDBInfo sysDBInfo, String tableName, String columnArrStr, int pageSize, int start, int end) {

        return executePageRecord(sysDBInfo, pagingSql(sysDBInfo.getDbType(), tableName, sysDBInfo.getDbTableSchema(), columnArrStr, pageSize, start, end));
    }

    /**
     * 带排序
     *
     * @param sysDBInfo
     * @param tableName
     * @param columnArrStr
     * @param pageSize
     * @param start
     * @param end
     * @return
     */
    public static List<List<Object>> executePageSort(SysDBInfo sysDBInfo, String tableName, String columnArrStr, int pageSize, int start, int end, String sortField, String descType) {

        return executePageRecord(sysDBInfo, pagingSqlSort(sysDBInfo.getDbType(), tableName, sysDBInfo.getDbTableSchema(), columnArrStr, pageSize, start, end, sortField, descType));
    }


    /**
     * 按照数字进行排序
     *
     * @param sysDBInfo
     * @param tableName
     * @param columnArrStr
     * @param pageSize
     * @param start
     * @param end
     * @param sortField
     * @param descType
     * @return
     */
    public static List<List<Object>> executePageSortByNumber(SysDBInfo sysDBInfo, String tableName, String columnArrStr, int pageSize, int start, int end, String sortField, String descType) {

        return executePageRecord(sysDBInfo, pagingSqlSortByNumber(sysDBInfo.getDbType(), tableName, sysDBInfo.getDbTableSchema(), columnArrStr, pageSize, start, end, sortField, descType));
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 查询分页记录结果, 不含总记录数
     * @Date: 10:05 2018/5/25
     */
    public static List<List<Object>> executePageRecord(SysDBInfo sysDBInfo, String querySql) {
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
            int rowNum = data.getColumnCount();
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


    /**
     * @return
     * @Author: MR LIS
     * @Description: 总记录数sql
     * @Date: 9:56 2018/5/25
     */
    public static String pagingCountSql(String dbType, String tableName, String tableSchema) {
        if (StringUtils.isEmpty(dbType) || StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("sql或者数据库类型不能为空！");
        }
        String countSql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(dbType) || DBTypeEnum.DB_TIDB.getDbName().equals(dbType)) {
            countSql = "select count(*) as count from " + tableName + " t";

        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(dbType)) {
            countSql = "select count(*) as count from " + tableSchema+"."+tableName + " t";

        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(dbType)) {
            countSql = "select count(*) as count from " + tableSchema + "." + tableName + " t";
        }

        return countSql;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 带条件的总记录数sql
     * @Date: 9:56 2018/12/14
     */
    public static String pagingCountSqlWithCondition(String dbType, String tableName, String tableSchema, List<ConditionVo> conditionVos) {
        if (StringUtils.isEmpty(dbType) || StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("sql或者数据库类型不能为空！");
        }
        String conditionSql = geneConditionSql(conditionVos);
        String countSql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(dbType) || DBTypeEnum.DB_TIDB.getDbName().equals(dbType)) {
            countSql = "select count(*) as count from " + tableName;

        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(dbType)) {
            countSql = "select count(*) as count from " + tableSchema+"."+tableName;

        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(dbType)) {
            countSql = "select count(*) as count from " + tableSchema + "." + tableName;
        }

        //加上条件sql
        countSql += conditionSql;

        return countSql;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 分页sql
     * @Date: 9:56 2018/5/25
     */
    public static String pagingSql(String dbType, String tableName, String tableSchema, Integer size, Integer start, Integer end) {
        if (StringUtils.isEmpty(dbType) || StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("sql或者数据库类型不能为空！");
        }
        String querySql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(dbType) || DBTypeEnum.DB_TIDB.getDbName().equals(dbType)) {
            querySql = "select * from " + tableName + " limit " + start + "," + size;

        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select * from (select T.*,ROWNUM RN from " + tableSchema+"."+tableName + "  T where ROWNUM <= " + end + ") where RN >" + start;

        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select * from " + tableSchema + "." + tableName + "  limit " + size + " offset  " + start;

        }
        return querySql;
    }

    /**
     * 针对绑定
     *
     * @Author: MR LIS
     * @Description: 分页sql
     * @Date: 9:56 2018/5/25
     */
    public static String pagingSqlSpecial(String dbType, String tableName, String columnArr, String tableSchema, Integer size, Integer start, Integer end) {
        if (StringUtils.isEmpty(dbType) || StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("sql或者数据库类型不能为空！");
        }
        String querySql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(dbType) || DBTypeEnum.DB_TIDB.getDbName().equals(dbType)) {
            querySql = "select " + columnArr + " from " + tableName + " limit " + start + "," + size;

        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select " + columnArr + " from (select T.*,ROWNUM RN from " + tableSchema+"."+tableName + "  T where ROWNUM <= " + end + ") where RN >" + start;

        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select " + columnArr + " from " + tableSchema + "." + tableName + " where num_id>=" + start + " and num_id <=" + end;

        }
        return querySql;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 分页sql
     * @Date: 9:56 2018/5/25
     */
    public static String pagingSqlWithCondition(String dbType, String tableName, String columnArr, List<ConditionVo> conditionVos, String tableSchema, Integer size, Integer start, Integer end) {
        if (StringUtils.isEmpty(dbType) || StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("sql或者数据库类型不能为空！");
        }
        String conditionSql = geneConditionSql(conditionVos);
        String querySql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(dbType) || DBTypeEnum.DB_TIDB.getDbName().equals(dbType)) {
            querySql = "select " + columnArr + " from " + tableName + conditionSql + " limit " + start + "," + size;

        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(dbType)) {
            if (conditionVos.isEmpty()) {
                querySql = "select " + columnArr + " from (select T.*,ROWNUM RN from " + tableSchema+"."+tableName + "  T where ROWNUM <= " + end + ") where RN >" + start;
            } else {
                querySql = "select " + columnArr + " from (select T.*,ROWNUM RN from " + tableSchema+"."+tableName + "  T " + conditionSql + " and ROWNUM <= " + end + ") where RN >" + start;
            }

        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select " + columnArr + " from " + tableSchema + "." + tableName + conditionSql + "  limit " + size + " offset  " + start;

        }
        return querySql;
    }

    /**
     * 生成条件sql
     *
     * @param conditionVos
     * @return
     */
    private static String geneConditionSql(List<ConditionVo> conditionVos) {
        String conditionSql = "";
        if (conditionVos != null) {
            int i = 0;
            for (ConditionVo vo : conditionVos) {
                if (i == 0) {
                    conditionSql += " where ";
                } else {
                    conditionSql += " and ";
                }
                conditionSql += vo.getKey() + "=" + "'" + vo.getValue() + "'";
                i++;
            }
        }
        return conditionSql;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 分页sql, 根据指定的列进行分页查询
     * @Date: 9:56 2018/5/25
     */
    public static String pagingSql(String dbType, String tableName, String tableSchema, String columnArrStr, Integer size, Integer start, Integer end) {
        if (StringUtils.isEmpty(dbType) || StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("sql或者数据库类型不能为空！");
        }
        String querySql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(dbType) || DBTypeEnum.DB_TIDB.getDbName().equals(dbType)) {
            querySql = "select " + columnArrStr + " from " + tableName + " limit " + start + "," + size;

        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select " + columnArrStr + " from (select T.*,ROWNUM RN from " + tableSchema+"."+tableName + "  T where ROWNUM <= " + end + ") where RN >" + start;

        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select " + columnArrStr + " from " + tableSchema + "." + tableName + " order by 1 limit " + size + " offset  " + start;

        }

        return querySql;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 分页sql, 根据指定的列进行分页查询+排序
     * @Date: 9:56 2018/5/25
     */
    public static String pagingSqlSort(String dbType, String tableName, String tableSchema, String columnArrStr, Integer size, Integer start, Integer end, String sortField, String descType) {
        if (StringUtils.isEmpty(dbType) || StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("sql或者数据库类型不能为空！");
        }
        String sortSql = "";
        if (StringUtils.isNotBlank(sortField)) {
            sortSql += " order by " + sortField + " " + descType;
        }

        String querySql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(dbType) || DBTypeEnum.DB_TIDB.getDbName().equals(dbType)) {
            querySql = "select " + columnArrStr + " from " + tableName + sortSql + " limit " + start + "," + size;

        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select " + columnArrStr + " from (select T.*,ROWNUM RN from " + tableSchema+"."+tableName + "  T where ROWNUM <= " + end + ") where RN >" + start + sortSql;

        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(dbType)) {
            querySql = "select " + columnArrStr + " from " + tableSchema + "." + tableName + sortSql + " limit " + size + " offset  " + start;

        }
        return querySql;
    }

    public static String pagingSqlSortByNumber(String dbType, String tableName, String tableSchema, String columnArrStr, Integer size, Integer start, Integer end, String sortField, String descType) {
        if (StringUtils.isEmpty(dbType) || StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("sql或者数据库类型不能为空！");
        }
        String sortSql = "";
        if (StringUtils.isNotBlank(sortField)) {
            sortSql += " order by CAST(" + sortField + " as DECIMAL) " + descType;
        }

        String querySql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(dbType) || DBTypeEnum.DB_TIDB.getDbName().equals(dbType)) {
            querySql = "select " + columnArrStr + " from " + tableName + sortSql + " limit " + start + "," + size;

        }
        logger.info(" --数字排序的sql是---- {}", querySql);
        return querySql;
    }


    /**
     * mysql与oracle参考对比，参考：https://blog.csdn.net/superit401/article/details/51565119
     */
    //int集合
    private static List<String> intList = new ArrayList<String>() {{
        //mysql数据库
        add("int");
        add("integer");
        add("tinyint");
        add("smallint");
        add("bigint");
        add("bigint");
        add("mediumint");
        add("numeric");
        //oracle数据库
        add("number");
    }};
    //double 集合
    private static List<String> floatList = new ArrayList<String>() {{
        //mysql数据库
        add("float");
        add("double");
        add("decimal");
        add("real");
        //es使用
        add("geo");
    }};

    //date 集合
    private static List<String> dateList = new ArrayList<String>() {{

        //mysql数据库
        add("date");
        add("datetime");
        add("time");
        add("timestamp");

    }};
    /**
     * string 集合
     */
    private static List<String> stringList = new ArrayList<String>() {{
        //mysql数据库
        add("char");
        add("varchar");
        add("text");
        add("tinytext");
        add("enum");
        //oracle数据库
    }};

    /**
     * @return
     * @Author: MR LIS
     * @Description: 转换数据类型
     * @Date: 10:56 2018/5/28
     */
    public static String convertDataType(String dataType) {
        for (String s : intList) {
            if (dataType.toLowerCase().indexOf(s.toLowerCase()) != -1) {
                return DbDataTypeEnum.NUMBER.getType();
            }
        }

        for (String s : floatList) {
            if (dataType.toLowerCase().indexOf(s.toLowerCase()) != -1)
            {
                return DbDataTypeEnum.FLOAT.getType();
            }
        }
        for (String s : dateList) {
            if (dataType.toLowerCase().indexOf(s.toLowerCase()) != -1) {
                return DbDataTypeEnum.DATE.getType();
            }
        }
        for (String s : stringList) {
            if (dataType.toLowerCase().indexOf(s.toLowerCase()) != -1) {
                return DbDataTypeEnum.STRING.getType();
            }
        }

        return DbDataTypeEnum.STRING.getType();
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据数据库连接+库名 得到对应的所有表信息
     * @Date: 9:43 2018/5/30
     */
    public static List<SourceDataInfoShowVO> getDbTableInfos(SysDBInfo sysDBInfo) {
        List<SourceDataInfoShowVO> showVOs = new ArrayList<>();
        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        SourceDataInfoShowVO showVO = null;
        try {
            stmt = conn.prepareStatement(assembleTableSql(sysDBInfo));
            rs = stmt.executeQuery();
            while (rs.next()) {
                showVO = new SourceDataInfoShowVO();
                SourceDataInfoVO sourceVO = new SourceDataInfoVO(sysDBInfo.getDbinfoId(), rs.getString(TABLE_NAME), rs.getString(TABLE_COMMENT) == null ? "" : rs.getString(TABLE_COMMENT));
                showVO.setCount(rs.getLong(TABLE_ROWNUM));
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


    /**
     * @return
     * @Author: MR LIS
     * @Description: 拼接获取库对应的所有表信息
     * mysql、oracle根据库名去查找下面所有自己创建的表，postgres根据模式去找，同一个库下的不同模式不要有重复的表,postgres连接时已经制定了库
     * @Date: 9:56 2018/5/30
     */
    private static String assembleTableSql(SysDBInfo sysDBInfo) {
        if (StringUtils.isEmpty(sysDBInfo.getDbType())) {
            throw new RuntimeException("数据库类型不能为空！");
        }
        return getAssembleSqlString(sysDBInfo, TABLE_MYSQL_PREFIX, TABLE_ORACLE_PREFIX, TABLE_POSTGRES_PREFIX);
    }


    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据数据库连接+库名 得到对应的所有表记录求和总记录数
     * @Date: 9:43 2018/5/30
     */
    public static int getDbRowTotalCount(SysDBInfo sysDBInfo) {

        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int totalCount = 0;
        try {
            stmt = conn.prepareStatement(assembleROWCountSql(sysDBInfo));
            rs = stmt.executeQuery();
            while (rs.next()) {
                totalCount = rs.getInt(ROW_DB_TOTAL_COUNT);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, rs);
        }

        return totalCount;

    }


    /**
     * @return
     * @Author: MR LIS
     * @Description: 拼接获取库对应的所有表记录求和总记录数
     * mysql、oracle根据库名去查找下面所有自己创建的表，postgres根据模式去找，同一个库下的不同模式不要有重复的表,postgres连接时已经制定了库
     * @Date: 9:56 2018/5/30
     */
    private static String assembleROWCountSql(SysDBInfo sysDBInfo) {
        if (StringUtils.isEmpty(sysDBInfo.getDbType())) {
            throw new RuntimeException("数据库类型不能为空！");
        }
        return getAssembleSqlString(sysDBInfo, ROW_COUNT_MYSQL_PREFIX, ROW_COUNT_ORACLE_PREFIX, ROW_COUNT_POSTGRES_PREFIX);
    }


    /**
     * @return
     * @Author: MR LIS
     * @Description: 删表操作
     * @Date: 10:15 2018/6/1
     */
    public static void dropTable(SysDBInfo sysDBInfo, String tableName) {
        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(assembleDropTableSql(sysDBInfo.getDbType(), tableName, sysDBInfo.getDbTableSchema()));
            stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, rs);
        }
    }

    /**
     * 拼接删除表sql
     *
     * @param dbType
     * @param tableName
     * @return
     */
    private static String assembleDropTableSql(String dbType, String tableName, String tableSchema) {
        String dropSql = "";
        //postgres表名需要加引号
        if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(dbType)) {
            dropSql += DROP_TABLE_SQL + tableSchema + "." + "\"" + tableName + "\"";
        } else {
            dropSql += DROP_TABLE_SQL + tableName;
        }
        return dropSql;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据数据库连接+库名 得到库对应的表个数
     * @Date: 9:43 2018/5/30
     */
    public static int getDbTableTotalCount(SysDBInfo sysDBInfo) {

        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int totalCount = 0;
        try {
            stmt = conn.prepareStatement(assembleTableCountSql(sysDBInfo));
            rs = stmt.executeQuery();
            while (rs.next()) {
                totalCount = rs.getInt(TABLE_DB_TOTAL_COUNT);
            }

        } catch (Exception e) {
            logger.error("getDbTableTotalCount>>> {}:{}/{},异常:{}", sysDBInfo.getDbIp(), sysDBInfo.getDbPort(), sysDBInfo.getDbServerName(), e.getMessage());
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, rs);
        }

        return totalCount;

    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 拼接获取库对应的所有表个数
     * mysql、oracle根据库名去查找下面所有自己创建的表，postgres根据模式去找，同一个库下的不同模式不要有重复的表,postgres连接时已经制定了库
     * @Date: 9:56 2018/5/30
     */
    private static String assembleTableCountSql(SysDBInfo sysDBInfo) {
        if (StringUtils.isEmpty(sysDBInfo.getDbType())) {
            throw new RuntimeException("数据库类型不能为空！");
        }
        return getAssembleCountSql(sysDBInfo, TABLE_COUNT_MYSQL_PREFIX, TABLE_COUNT_ORACLE_PREFIX, TABLE_COUNT_POSTGRES_PREFIX);
    }

    /**
     * 拼接公共部分
     *
     * @param mysqlPrefix
     * @param oraclePrefix
     * @param postgresPrefix
     * @return
     */
    private static String getAssembleCountSql(SysDBInfo sysDBInfo, String mysqlPrefix, String oraclePrefix, String postgresPrefix) {
        String countSql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(sysDBInfo.getDbType()) || DBTypeEnum.DB_TIDB.getDbName().equals(sysDBInfo.getDbType())) {
            countSql = mysqlPrefix + "'" + sysDBInfo.getDbServerName() + "'";
        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(sysDBInfo.getDbType())) {
            countSql = oraclePrefix + "'" + sysDBInfo.getDbTableSchema() + "'";
        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(sysDBInfo.getDbType())) {
            countSql = postgresPrefix + "'" + sysDBInfo.getDbTableSchema() + "'";
        }
//        logger.info("countSql:"+countSql);
        return countSql;
    }

    /**
     * 拼接公共部分
     *
     * @param mysqlPrefix
     * @param oraclePrefix
     * @param postgresPrefix
     * @return
     */
    private static String getAssembleSqlString(SysDBInfo sysDBInfo, String mysqlPrefix, String oraclePrefix, String postgresPrefix) {
        String countSql = "";
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(sysDBInfo.getDbType()) || DBTypeEnum.DB_TIDB.getDbName().equals(sysDBInfo.getDbType())) {
            countSql = mysqlPrefix + "'" + sysDBInfo.getDbServerName() + "'";
        } else if (DBTypeEnum.DB_ORACLE.getDbName().equalsIgnoreCase(sysDBInfo.getDbType())) {
            countSql = oraclePrefix + "'" + sysDBInfo.getDbTableSchema() + "'";
        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(sysDBInfo.getDbType())) {
            countSql = postgresPrefix + "'" + sysDBInfo.getDbTableSchema() + "'" + " AND r.relkind = '" + sysDBInfo.getDbRelkind() + "'";
        }
//        logger.info("countSql:"+countSql);
        return countSql;

    }

    /**
     * 关闭连接
     *
     * @param stmt
     * @param rs
     */
    private synchronized static void closeConn(Connection conn, PreparedStatement stmt, ResultSet rs) {
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

        /**
         * 连接数减去1
         */
        int current = createCounter.decrementAndGet();
        //判断是否小于总连接的个数，并通知,只有一个线程可以拿到锁
        if (current < activeSize.get()) {
            GenDBUtils.class.notify();
        }
    }

    /**
     * 根据postgresql的模式和表名进行表清理
     *
     * @param sysDBInfo
     * @param tableEname
     */
    public static void vacuumPostgrepTable(SysDBInfo sysDBInfo, String tableEname) {
        Connection conn = getConnection(sysDBInfo);
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("vacuum " + sysDBInfo.getDbTableSchema() + "." + tableEname);
            stmt.executeUpdate();

        } catch (Exception e) {
            logger.error("vacuumPostgrepTable>>> {}:{}/{},异常:{}", sysDBInfo.getDbIp(), sysDBInfo.getDbPort(), sysDBInfo.getDbServerName(), e.getMessage());
            e.printStackTrace();
        } finally {
            closeConn(conn, stmt, null);
        }
    }
}
