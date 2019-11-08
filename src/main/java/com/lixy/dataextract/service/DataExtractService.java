package com.lixy.dataextract.service;

import com.lixy.dataextract.entity.CommitTableRecord;
import com.lixy.dataextract.entity.SysDBInfo;
import com.lixy.dataextract.vo.*;
import com.lixy.dataextract.vo.page.SpringPageVo;

import java.util.List;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public interface DataExtractService {


    /**
     * 获取db连接分页
     * @param record
     * @return
     */
    SpringPageVo<SysDBInfo> findDbPage(DbSearchVo record);

    /**
     * 已提交表分页列表
     * @param record
     * @return
     */
    SpringPageVo<CommitTableRecord> findCommitTablePage(CommitTableSearchVo record);

    /**
     * 根据数据库连接id获取所有表
     * @param searchVo
     * @return
     */
    SourceTablePageVo getTableListByDbId(SourceTableSearchVo searchVo);

    /**
     * 保存提交列表
     * @param record
     */
    void saveCommitTableRecord(CommitTableRecord record);

    /**
     * 添加任务
     */
    void batchAddHandlerTask(List<HandlerTaskVo> taskVos);

    /**
     * 任务列表
     * @param record
     * @return
     */
    SpringPageVo<TaskShowVo> findTaskPage(TaskSearchVo record);

    /**
     * 任务状态列表
     * @param record
     * @return
     */
    SpringPageVo<TaskStatusShowVo> findTaskStatusPage(TaskStatusSearchVo record);


    /**
     * 启动任务
     * @param taskIds
     */
    void startTasks(Integer[] taskIds);
    /**
     * 开启任务
     * @param taskId
     */
    void executeTask(Integer taskId);

    /**
     * 更新任务状态记录
     * @param handlerId
     */
    void updateTaskStatus(Integer handlerId,String beginIndex,String endIndex);

    /**
     * 删除任务
     * @param taskIds
     */
    void batchDelHandlerTask(List<Integer> taskIds);

    /**
     * 停止任务
     * @param taskIds
     */
    void stopTasks(Integer[] taskIds);

    /**
     * 删除提交表记录
     * @param commitIds
     */
    void deleteCommitTable(Integer[] commitIds);

    /**
     * 任务状态记录强制失败
     * @param handlerId
     */
    void taskStatusRecordforcedFailure(Integer handlerId);

    /**
     * 数据库连接删除
     * @param dbId
     */
    void deleteDbById(Integer dbId);
}
