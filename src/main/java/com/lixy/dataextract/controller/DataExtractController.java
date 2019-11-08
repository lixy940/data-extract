package com.lixy.dataextract.controller;

import com.lixy.dataextract.dao.SysDBInfoMapper;
import com.lixy.dataextract.entity.CommitTableRecord;
import com.lixy.dataextract.entity.SysDBInfo;
import com.lixy.dataextract.enums.ColumnTypeEnum;
import com.lixy.dataextract.enums.DBTypeEnum;
import com.lixy.dataextract.enums.TaskStatusEnum;
import com.lixy.dataextract.enums.TaskTypeEnum;
import com.lixy.dataextract.service.CommonService;
import com.lixy.dataextract.service.DataExtractService;
import com.lixy.dataextract.vo.*;
import com.lixy.dataextract.vo.page.SpringPageVo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
@Api(tags = {"数据库连接配置"})
@RestController
@RequestMapping("/dataextract")
public class DataExtractController {

    @Autowired
    private DataExtractService dataExtractService;

    @Autowired
    private CommonService commonService;


    @Autowired
    private SysDBInfoMapper sysDBInfoMapper;


    /**
     * 已提交表分页列表
     *
     * @param record
     * @return
     */
    @ApiOperation(value = "数据库连接列表", notes = "数据库连接列表")
    @PostMapping("findDbPage")
    public ResponseResult findDbPage(@RequestBody DbSearchVo record) {
        ResponseResult result = new ResponseResult();
        SpringPageVo<SysDBInfo> pageVo = dataExtractService.findDbPage(record);
        result.setData(pageVo);
        return result;
    }

    @ApiOperation(value = "获取数据库连接类型列表", notes = "获取数据库连接类型列表")
    @GetMapping("/getDbypeList")
    public ResponseResult getDbypeList() {
        ResponseResult result = new ResponseResult();
        result.setData(DBTypeEnum.getDbTypeList());
        return result;
    }

    /**
     * 保存数据库连接
     *
     * @return
     * @Author: MR LIS
     * @Date: 16:24 2018/7/30
     */
    @ApiOperation(value = "测试数据库连接", notes = "测试数据库连接")
    @PostMapping(value = "/testDbLink")
    public ResponseResult testDbLink(@RequestBody DataSourceSaveVO dbInfo) {
        ResponseResult result = new ResponseResult();
        result.setData(commonService.getDataSourceStatus(dbInfo));
        return result;
    }

    /**
     * 保存数据库连接
     *
     * @return
     * @Author: MR LIS
     * @Date: 16:24 2018/7/30
     */
    @ApiOperation(value = "保存数据库连接", notes = "保存数据库连接")
    @PostMapping(value = "/saveDbInfo")
    public ResponseResult saveDbInfo(@RequestBody SysDBInfo sysDBInfo) {
        ResponseResult result = new ResponseResult();
        sysDBInfo.setStatus(1);
        sysDBInfoMapper.insert(sysDBInfo);
        return result;
    }

    @ApiOperation(value = "获取指定dbId对应的连接对象", notes = "获取指定dbId对应的连接对象")
    @PostMapping(value = "/findDbById")
    public ResponseResult findDbById(Integer dbId) {
        ResponseResult result = new ResponseResult();
        SysDBInfo sysDBInfo = sysDBInfoMapper.selectByPrimaryKey(dbId);
        result.setData(sysDBInfo);
        return result;
    }

    @ApiOperation(value = "删除数据库连接", notes = "删除数据库连接")
    @GetMapping(value = "/deleteDbById")
    public ResponseResult deleteDbById(Integer dbId) {
        ResponseResult result = new ResponseResult();
        dataExtractService.deleteDbById(dbId);
        return result;
    }


    /**
     * 获取所有的数据库连接
     *
     * @return
     * @Author: MR LIS
     * @Date: 16:24 2018/7/30
     */
    @ApiOperation(value = "获取所有的数据库连接", notes = "获取所有的数据库连接")
    @PostMapping(value = "/getSysDbInfoList")
    public ResponseResult getSysDbInfoList() {
        ResponseResult result = new ResponseResult();
        List<SysDBInfo> sysDBInfos = sysDBInfoMapper.selectAll();
        result.setData(sysDBInfos);
        return result;
    }


    @ApiOperation(value = "根据数据库连接id获取所有表", notes = "获取所有的数据库连接")
    @PostMapping(value = "/getTableListByDbId")
    public ResponseResult getTableListByDbId(@RequestBody SourceTableSearchVo record) {
        ResponseResult result = new ResponseResult();
        SourceTablePageVo data = dataExtractService.getTableListByDbId(record);
        result.setData(data);
        return result;
    }

    @ApiOperation(value = "获取列类型列表", notes = "获取列类型列表")
    @GetMapping("/getColumnTypeList")
    public ResponseResult getColumnTypeList() {
        ResponseResult result = new ResponseResult();
       result.setData(ColumnTypeEnum.getList());
        return result;
    }


    @ApiOperation(value = "保存提交表记录", notes = "保存提交表记录")
    @PostMapping("/saveCommitTableRecord")
    public ResponseResult saveCommitTableRecord(@RequestBody CommitTableRecord record) {
        ResponseResult result = new ResponseResult();
        dataExtractService.saveCommitTableRecord(record);
        return result;
    }

    @ApiOperation(value = "删除提交表记录", notes = "删除提交表记录")
    @PostMapping("/deleteCommitTable")
    public ResponseResult deleteCommitTable(@RequestBody Integer[] commitIds) {
        ResponseResult result = new ResponseResult();
        dataExtractService.deleteCommitTable(commitIds);
        return result;
    }

    /**
     * 已提交表分页列表
     *
     * @param record
     * @return
     */
    @ApiOperation(value = "已提交表分页列表", notes = "已提交表分页列表")
    @PostMapping("findCommitTablePage")
    public ResponseResult findCommitTablePage(@RequestBody CommitTableSearchVo record) {
        ResponseResult result = new ResponseResult();
        SpringPageVo<CommitTableRecord> pageVo = dataExtractService.findCommitTablePage(record);
        result.setData(pageVo);
        return result;
    }


    /**
     * 批量添加已提交任务列表
     *
     * @return
     */
    @ApiOperation(value = "添加任务", notes = "添加任务")
    @PostMapping("batchAddHandlerTask")
    public ResponseResult batchAddHandlerTask(@RequestBody HandlerTaskVo[] taskVos) {
        ResponseResult result = new ResponseResult();
        dataExtractService.batchAddHandlerTask(Arrays.asList(taskVos));
        return result;
    }

    @ApiOperation(value = "删除任务", notes = "添加任务")
    @PostMapping("batchDelHandlerTask")
    public ResponseResult batchDelHandlerTask(@RequestBody Integer[] taskIds) {
        ResponseResult result = new ResponseResult();
        dataExtractService.batchDelHandlerTask(Arrays.asList(taskIds));
        return result;
    }


    @ApiOperation(value = "启动任务", notes = "启动任务")
    @PostMapping("startTasks")
    public ResponseResult startTasks(@RequestBody Integer[] taskIds) {
        ResponseResult result = new ResponseResult();
        dataExtractService.startTasks(taskIds);
        return result;
    }

    @ApiOperation(value = "停止任务", notes = "停止任务")
    @PostMapping("stopTasks")
    public ResponseResult stopTasks(@RequestBody Integer[] taskIds) {
        ResponseResult result = new ResponseResult();
        dataExtractService.stopTasks(taskIds);
        return result;
    }


    @ApiOperation(value = "任务列表", notes = "任务列表")
    @PostMapping("findTaskPage")
    public ResponseResult findTaskPage(@RequestBody TaskSearchVo record) {
        ResponseResult result = new ResponseResult();
        SpringPageVo<TaskShowVo> pageVo = dataExtractService.findTaskPage(record);
        result.setData(pageVo);
        return result;
    }


    @ApiOperation(value = "任务状态列表", notes = "任务状态列表")
    @PostMapping("findTaskStatusPage")
    public ResponseResult findTaskStatusPage(@RequestBody TaskStatusSearchVo record) {
        ResponseResult result = new ResponseResult();
        SpringPageVo<TaskStatusShowVo> pageVo = dataExtractService.findTaskStatusPage(record);
        result.setData(pageVo);
        return result;
    }

    @ApiOperation(value = "任务状态记录强制失败", notes = "任务状态记录强制失败")
    @GetMapping("taskStatusRecordforcedFailure")
    public ResponseResult taskStatusRecordforcedFailure(Integer handlerId) {
        ResponseResult result = new ResponseResult();
        dataExtractService.taskStatusRecordforcedFailure(handlerId);
        return result;
    }


    @ApiOperation(value = "任务状态类型", notes = "任务状态类型")
    @GetMapping("taskStatusTypeList")
    public ResponseResult taskStatusTypeList() {
        ResponseResult result = new ResponseResult();
        result.setData(TaskStatusEnum.getList());
        return result;
    }
    @ApiOperation(value = "任务周期类型", notes = "任务周期类型")
    @GetMapping("taskPeriodTypeList")
    public ResponseResult taskPeriodTypeList() {
        ResponseResult result = new ResponseResult();
        result.setData(TaskTypeEnum.getList());
        return result;
    }


}
