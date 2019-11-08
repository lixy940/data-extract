package com.lixy.dataextract.controller;

import com.lixy.dataextract.dao.SysDBInfoMapper;
import com.lixy.dataextract.entity.SysDBInfo;
import com.lixy.dataextract.enums.DBTypeEnum;
import com.lixy.dataextract.service.CommonService;
import com.lixy.dataextract.vo.ColumnInfoVO;
import com.lixy.dataextract.vo.ResponseResult;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: MR LIS
 * @Description:
 * @Date: Create in 16:40 2018/5/25
 * @Modified By:
 */
@Api(tags = {"数据库访问公共接口"})
@RestController
@RequestMapping("/sandcommon")
public class CommonController {

    private final static Logger logger = LoggerFactory.getLogger(CommonController.class);

    @Autowired
    private CommonService commonService;

    @Autowired
    private SysDBInfoMapper sysDBInfoMapper;



    /**
     * @return
     * @Author: MR LIS
     * @Description: 根据数据库id、表名获取表各列的列名、注释及数据类型
     * @Date: 16:46 2018/5/25
     */
    @ApiOperation(value = "获取表所有列信息", notes = "根据数据库id、表名获取表各列的列名、注释及数据类型", consumes = "application/json", response = ResponseResult.class)
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "path", name = "dbId", dataType = "Integer", required = true, value = "数据库配置id", defaultValue = ""),
            @ApiImplicitParam(paramType = "path", name = "tableName", dataType = "String", required = true, value = "数据库表名", defaultValue = "")
    })
    @GetMapping("getAllColumnInfo/{dbId}/{tableName}")
    public ResponseResult getAllColumnInfo(@PathVariable("dbId") Integer dbId, @PathVariable("tableName") String tableName) {
        ResponseResult responseResult = new ResponseResult();
        List<ColumnInfoVO> allColumnInfo = commonService.getAllColumnInfo(dbId, tableName);
        responseResult.setData(allColumnInfo);

        return responseResult;
    }


    /**
     * @return
     * @Author: MR LIS
     * @Description: 获取分页列表信息
     * @Date: 16:46 2018/5/25
     */
    @ApiOperation(value = "获取分页列表总记录数", notes = "获取分页列表总记录数", consumes = "application/json", response = ResponseResult.class)
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "path", name = "dbId", dataType = "Integer", required = true, value = "数据库id", defaultValue = ""),
            @ApiImplicitParam(paramType = "path", name = "tableName", dataType = "String", required = true, value = "数据库表名", defaultValue = "")
    })
    @GetMapping("executePageTotalCount/{dbId}/{tableName}")
    public ResponseResult executePageTotalCount(@PathVariable("dbId") Integer dbId, @PathVariable("tableName") String tableName) {
        ResponseResult responseResult = new ResponseResult();
        int totalCount = commonService.executePageTotalCount(dbId, tableName);
        responseResult.setData(totalCount);

        return responseResult;
    }

    /**
     * @return
     * @Author: MR LIS
     * @Description: 获取分页列表信息, 不进行总记录数查询
     * @Date: 16:46 2018/5/25
     */
    @ApiOperation(value = "获取不含总记录数的分页列表", notes = "获取分页列表信息,不返回总记录数", consumes = "application/json", response = ResponseResult.class)
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "path", name = "dbId", dataType = "Integer", required = true, value = "数据库id", defaultValue = ""),
            @ApiImplicitParam(paramType = "path", name = "tableName", dataType = "String", required = true, value = "数据库表名", defaultValue = ""),
    })
    @GetMapping("executePageNotCount/{dbId}/{tableName}")
    public ResponseResult executePageNotCount(@PathVariable("dbId") Integer dbId, @PathVariable("tableName") String tableName) {
        ResponseResult responseResult = new ResponseResult();

        /**
         * 本地数据源
         */
        List<ColumnInfoVO> allColumnInfo = commonService.getAllColumnInfo(dbId, tableName);
        SysDBInfo sysDBInfo = sysDBInfoMapper.selectByPrimaryKey(dbId);
        String columnArr = null;
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(sysDBInfo.getDbType()) || DBTypeEnum.DB_TIDB.getDbName().equals(sysDBInfo.getDbType())) {
            List<String> collect = allColumnInfo.stream().map(o -> "`" + o.getColumnEname() + "`").collect(Collectors.toList());
            columnArr = StringUtils.join(collect, ",");
        } else if (DBTypeEnum.DB_POSTGRESQL.getDbName().equalsIgnoreCase(sysDBInfo.getDbType())) {
            List<String> collect = allColumnInfo.stream().map(o -> "\"" + o.getColumnEname() + "\"").collect(Collectors.toList());
            columnArr = StringUtils.join(collect, ",");
        } else {
            List<String> collect = allColumnInfo.stream().map(o -> o.getColumnEname()).collect(Collectors.toList());
            columnArr = StringUtils.join(collect, ",");
        }


        List<List<Object>> dataList = commonService.executePageQueryColumnRecord(dbId, tableName, columnArr, 1, 10);
        responseResult.setData(dataList);


        return responseResult;
    }




}
