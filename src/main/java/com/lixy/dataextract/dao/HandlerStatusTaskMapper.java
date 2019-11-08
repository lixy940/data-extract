package com.lixy.dataextract.dao;

import com.lixy.dataextract.entity.HandlerStatusTask;
import com.lixy.dataextract.vo.TaskStatusSearchVo;
import com.lixy.dataextract.vo.TaskStatusShowVo;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface HandlerStatusTaskMapper {

    int deleteByPrimaryKey(Integer handlerId);

    int insert(HandlerStatusTask record);

    int insertSelective(HandlerStatusTask record);

    HandlerStatusTask selectByPrimaryKey(Integer handlerId);

    int updateHandlerBySelective(HandlerStatusTask record);

    List<TaskStatusShowVo> findTaskStatusPage(TaskStatusSearchVo record);

    /**
     * 获取任务对应的最新一条记录
     *
     * @param taskId
     * @return
     */
    HandlerStatusTask selectLastRecordByTaskId(@Param("taskId") Integer taskId);

    /**
     * 根据任务id删除所有任务状态记录
     *
     * @param taskId
     */
    void deleteByTaskId(@Param("taskId") Integer taskId);
}