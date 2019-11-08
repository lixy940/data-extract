package com.lixy.dataextract.service.impl;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.lixy.dataextract.constants.StaticParameterUtils;
import com.lixy.dataextract.dao.CommitTableRecordMapper;
import com.lixy.dataextract.dao.HandlerStatusTaskMapper;
import com.lixy.dataextract.dao.HandlerTaskMapper;
import com.lixy.dataextract.dao.SysDBInfoMapper;
import com.lixy.dataextract.entity.CommitTableRecord;
import com.lixy.dataextract.entity.HandlerStatusTask;
import com.lixy.dataextract.entity.HandlerTask;
import com.lixy.dataextract.entity.SysDBInfo;
import com.lixy.dataextract.enums.BussinessException;
import com.lixy.dataextract.enums.DBTypeEnum;
import com.lixy.dataextract.enums.TaskStatusEnum;
import com.lixy.dataextract.enums.TaskTypeEnum;
import com.lixy.dataextract.quartz.JobConstant;
import com.lixy.dataextract.quartz.QuartzUtils;
import com.lixy.dataextract.quartz.job.MissionJobImpl;
import com.lixy.dataextract.service.CommonService;
import com.lixy.dataextract.service.DataExtractService;
import com.lixy.dataextract.service.H2DbService;
import com.lixy.dataextract.utils.GenDBUtils;
import com.lixy.dataextract.utils.KettleUtils;
import com.lixy.dataextract.vo.*;
import com.lixy.dataextract.vo.kettle.KettleCreateInfo;
import com.lixy.dataextract.vo.page.ListToSpringPageVo;
import com.lixy.dataextract.vo.page.SpringPageVo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
@Service
public class DataExtractServiceImpl implements DataExtractService {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final Semaphore SEMAPHORE = new Semaphore(StaticParameterUtils.CONCURRENT_SEMAPHORE_COUNT);

    @Autowired
    private CommonService commonService;

    @Autowired
    private H2DbService h2DbService;

    @Autowired
    private CommitTableRecordMapper commitTableRecordMapper;

    @Autowired
    private SysDBInfoMapper sysDBInfoMapper;

    @Autowired
    private HandlerTaskMapper handlerTaskMapper;

    @Autowired
    private HandlerStatusTaskMapper handlerStatusTaskMapper;

    @Autowired
    private QuartzUtils quartzUtils;


    @Override
    public SpringPageVo<SysDBInfo> findDbPage(DbSearchVo record) {
        PageHelper.startPage(record.getPageIndex(), record.getPageSize());
        List<SysDBInfo> records = sysDBInfoMapper.findDbPage(record);
        PageInfo<SysDBInfo> pageInfos = new PageInfo<>(records);
        return ListToSpringPageVo.listToPage(record.getPageIndex(), record.getPageSize(), pageInfos.getTotal(), pageInfos.getList());
    }

    @Override
    public SpringPageVo<CommitTableRecord> findCommitTablePage(CommitTableSearchVo record) {
        PageHelper.startPage(record.getPageIndex(), record.getPageSize());
        List<CommitTableRecord> records = commitTableRecordMapper.findPage(record);
        PageInfo<CommitTableRecord> pageInfos = new PageInfo<>(records);
        return ListToSpringPageVo.listToPage(record.getPageIndex(), record.getPageSize(), pageInfos.getTotal(), pageInfos.getList());
    }

    @Override
    public SourceTablePageVo getTableListByDbId(SourceTableSearchVo pageQueryVo) {

        SysDBInfo config = sysDBInfoMapper.selectByPrimaryKey(pageQueryVo.getDbId());
        if (config.getDbType().equalsIgnoreCase(DBTypeEnum.DB_H2.getDbName())) {
            return h2DbService.findTablesBySchema(pageQueryVo);
        } else {
            List<SourceDataInfoShowVO> dbTableInfos = GenDBUtils.getDbTableInfos(config);
            return commonService.filterCondition(dbTableInfos, pageQueryVo);
        }
    }

    @Override
    public void saveCommitTableRecord(CommitTableRecord record) {

        int count = commitTableRecordMapper.findCountTableName(record.getTableName());
        if (count > 0) {
            throw new RuntimeException("该表已在提交表列表");
        }

        SysDBInfo sysDBInfo = sysDBInfoMapper.selectByPrimaryKey(record.getDbId());

        KettleCreateInfo createInfo = new KettleCreateInfo();
        createInfo.setTableName(record.getTableName());
        createInfo.setIncrementalCloumnName(record.getEtlCol());
        if (DBTypeEnum.DB_MYSQL.getDbName().equalsIgnoreCase(sysDBInfo.getDbType()) || DBTypeEnum.DB_TIDB.getDbName().equals(sysDBInfo.getDbType())) {
            createInfo.setOwner(sysDBInfo.getDbServerName());
        } else {
            createInfo.setOwner(sysDBInfo.getDbTableSchema());
        }
        List<ColumnInfoVO> allColumnInfo = commonService.getAllColumnInfo(record.getDbId(), record.getTableName());
        List<String> columnList = allColumnInfo.stream().map(ColumnInfoVO::getColumnEname).collect(Collectors.toList());
        createInfo.setCloumns(columnList.toArray(new String[0]));
        createInfo.setIncrementalCloumnCategory(record.getDataType());
        createInfo.setDatabaseId(record.getDbId());

        SysDBInfo localDb = sysDBInfoMapper.selectByPrimaryKey(StaticParameterUtils.LOCAL_STORAGE_PG_DB_ID);
        createInfo.setOwnerDestination(localDb.getDbTableSchema());
        createInfo.setHomePath(StaticParameterUtils.ETL_BASE_SERVER_PATH);
        DbConnInfo dbConnInfo = commonService.setDbConnInfo(localDb.getDbinfoId());
        createInfo.setDbConnInfo(dbConnInfo);
        //根据不同的数据连接，获取linux脚本
        switch (sysDBInfo.getDbType()) {
            case "oracle":
                String path = KettleUtils.createKettleFilesForOracle(createInfo);
                record.setPath(path);
                break;
            case "postgresql":
                String postPath = KettleUtils.createKettleFilesForPostgres(createInfo);
                record.setPath(postPath);
                break;
            case "mysql":
                String mysqlPath = KettleUtils.createKettleFilesForMysql(createInfo);
                record.setPath(mysqlPath);
                break;
            case "h2":
                String h2Path = KettleUtils.createKettleFilesForH2(createInfo);
                record.setPath(h2Path);
                break;
            default:
                throw new RuntimeException("数据库类型不存在");
        }

        commitTableRecordMapper.insert(record);
    }

    @Override
    public void batchAddHandlerTask(List<HandlerTaskVo> taskVos) {

        for (HandlerTaskVo taskVo : taskVos) {
            HandlerTask handlerTask = new HandlerTask();
            handlerTask.setCommitId(taskVo.getCommitId());
            handlerTask.setTaskType(taskVo.getTaskType());
            if (taskVo.getTaskType() == TaskTypeEnum.PERIOD.getCode()) {
                handlerTask.setCronExp(convertTaskDateTimeVoToCron(taskVo.getTaskDateTimeVo()));
                handlerTask.setTaskPeriod(convertTaskPeriodStr(taskVo.getTaskDateTimeVo()));
            }
            handlerTaskMapper.insertSelective(handlerTask);
            startTasks(new Integer[]{handlerTask.getTaskId()});
        }
    }

    @Override
    public SpringPageVo<TaskShowVo> findTaskPage(TaskSearchVo record) {
        PageHelper.startPage(record.getPageIndex(), record.getPageSize());
        List<TaskShowVo> taskPage = handlerTaskMapper.findTaskPage(record);
        PageInfo<TaskShowVo> pageInfos = new PageInfo<>(taskPage);
        return ListToSpringPageVo.listToPage(record.getPageIndex(), record.getPageSize(), pageInfos.getTotal(), pageInfos.getList());
    }

    @Override
    public SpringPageVo<TaskStatusShowVo> findTaskStatusPage(TaskStatusSearchVo record) {
        PageHelper.startPage(record.getPageIndex(), record.getPageSize());
        List<TaskStatusShowVo> taskPage = handlerStatusTaskMapper.findTaskStatusPage(record);
        PageInfo<TaskStatusShowVo> pageInfos = new PageInfo<>(taskPage);
        return ListToSpringPageVo.listToPage(record.getPageIndex(), record.getPageSize(), pageInfos.getTotal(), pageInfos.getList());
    }

    @Override
    public void startTasks(Integer[] taskIds) {
        for (Integer taskId : taskIds) {
            HandlerTask handlerTask = handlerTaskMapper.selectByPrimaryKey(taskId);
            /**
             * taskType: 1 永久一次的 2 周期执行
             */
            if (handlerTask.getTaskType() == TaskTypeEnum.ONCE.getCode()) {
                CompletableFuture.runAsync(() -> {
                    handlerTask.setTaskStatus(TaskStatusEnum.PROCESSING.getCode());
                    handlerTaskMapper.updateByPrimaryKeySelective(handlerTask);
                    executeTask(taskId);
                });
            } else {
                Map<String, Object> params = new HashMap<>();
                params.put(JobConstant.TASK_ID_KEY, taskId);
                quartzUtils.addJob(JobConstant.DATA_EXTRACT_TASK_JOB_PREFIX + handlerTask.getTaskId(), handlerTask.getCronExp(), MissionJobImpl.class, params);
                handlerTask.setTaskStatus(TaskStatusEnum.PROCESSING.getCode());
                handlerTaskMapper.updateByPrimaryKeySelective(handlerTask);
            }
        }
    }

    @Override
    public void executeTask(Integer taskId) {
        try {
            SEMAPHORE.acquire();
            logger.info("--------------taskId---{}", taskId);
            HandlerTask handlerTask = handlerTaskMapper.selectByPrimaryKey(taskId);

            HandlerStatusTask statusTask = handlerStatusTaskMapper.selectLastRecordByTaskId(taskId);
            /**
             * 1.如果最新一条为空或者状态已完成，直接生成一条新的任务状态
             * 2.如果存在一条最新的正在处理的任务状态，直接使用
             */

            //最新一条为空或者状态已完成，直接生成一条新的任务状态
            if (statusTask == null || statusTask.getStatus() == TaskStatusEnum.FINISHED.getCode()) {
                statusTask = new HandlerStatusTask();
                statusTask.setTaskId(taskId);
                statusTask.setStatus(TaskStatusEnum.PROCESSING.getCode());
                handlerStatusTaskMapper.insertSelective(statusTask);
            } else {
                logger.info("handlerId---{},------上一次任务执行还在进行中", statusTask.getHandlerId());
                SEMAPHORE.release();
                return;
            }

            //更新处理时间
            handlerTask.setLastHandleTime(new Date());
            handlerTaskMapper.updateByPrimaryKeySelective(handlerTask);

            CommitTableRecord commitTableRecord = commitTableRecordMapper.selectByPrimaryKey(handlerTask.getCommitId());

            //调用脚本执行同步
            KettleUtils.startKettleJob(StaticParameterUtils.ETL_BASE_SERVER_PATH, commitTableRecord.getPath(), statusTask.getHandlerId(), StaticParameterUtils.CALL_BACK_URL);

        } catch (Exception e) {
            logger.error("", e);
        }
    }

    @Override
    public void updateTaskStatus(Integer handlerId, String beginIndex, String endIndex) {
        try {
            HandlerStatusTask statusTask = handlerStatusTaskMapper.selectByPrimaryKey(handlerId);

            statusTask.setExecuteResult(StringUtils.isNotBlank(endIndex) ? 1 : 0);
            statusTask.setBeginIndex(beginIndex);
            statusTask.setEndIndex(endIndex);
            statusTask.setStatus(TaskStatusEnum.FINISHED.getCode());
            handlerStatusTaskMapper.updateHandlerBySelective(statusTask);
            //更新任务
            HandlerTask handlerTask = handlerTaskMapper.selectByPrimaryKey(statusTask.getTaskId());
            //如果非周期性任务，修改任务列表状态
            if (handlerTask.getTaskType() != TaskTypeEnum.PERIOD.getCode()) {
                handlerTask.setTaskStatus(TaskStatusEnum.FINISHED.getCode());
                handlerTaskMapper.updateByPrimaryKeySelective(handlerTask);
            }
        } finally {
            SEMAPHORE.release();
        }

    }

    @Override
    public void batchDelHandlerTask(List<Integer> taskIds) {
        for (Integer taskId : taskIds) {
            quartzUtils.deleteJob(JobConstant.DATA_EXTRACT_TASK_JOB_PREFIX + taskId);
            handlerTaskMapper.deleteByPrimaryKey(taskId);
            handlerStatusTaskMapper.deleteByTaskId(taskId);
        }
    }

    @Override
    public void stopTasks(Integer[] taskIds) {
        for (Integer taskId : taskIds) {

            //周期性的任务
            quartzUtils.deleteJob(JobConstant.DATA_EXTRACT_TASK_JOB_PREFIX + taskId);

            HandlerTask handlerTask = handlerTaskMapper.selectByPrimaryKey(taskId);
            handlerTask.setTaskStatus(TaskStatusEnum.PENDING.getCode());
            handlerTaskMapper.updateByPrimaryKeySelective(handlerTask);
        }
    }

    @Override
    public void deleteCommitTable(Integer[] commitIds) {
        for (Integer commitId : commitIds) {

            CommitTableRecord commitTableRecord = commitTableRecordMapper.selectByPrimaryKey(commitId);

            int taskCount = handlerTaskMapper.findCountByCommitId(commitId);
            if (taskCount > 0) {
                throw new BussinessException(201, "该表当前存在任务，不能删除");
            }

            Integer dbId = StaticParameterUtils.LOCAL_STORAGE_PG_DB_ID;
            SysDBInfo sysDBInfo = sysDBInfoMapper.selectByPrimaryKey(dbId);
            String tableName = StringUtils.isNotBlank(sysDBInfo.getDbTableSchema()) ? sysDBInfo.getDbTableSchema() + "." + commitTableRecord.getTableName() : commitTableRecord.getTableName();

            int count = commonService.getIsTableExistCount(dbId, commitTableRecord.getTableName());
            if (count > 0) {
                String sql = "drop table " + tableName;
                DbConnInfo dbConnInfo = commonService.setDbConnInfo(dbId);
                commonService.executeJdbc(dbConnInfo, sql);
                String seq = "drop sequence " + sysDBInfo.getDbTableSchema() + "_" + commitTableRecord.getTableName() + "_seq" + " CASCADE ";
                commonService.executeJdbc(dbConnInfo, seq);
            }

            commitTableRecordMapper.deleteByPrimaryKey(commitId);

        }
    }

    @Override
    public void taskStatusRecordforcedFailure(Integer handlerId) {
        try {
            HandlerStatusTask statusTask = handlerStatusTaskMapper.selectByPrimaryKey(handlerId);
            statusTask.setExecuteResult(0);
            statusTask.setStatus(TaskStatusEnum.FINISHED.getCode());
            handlerStatusTaskMapper.updateHandlerBySelective(statusTask);

            HandlerTask handlerTask = handlerTaskMapper.selectByPrimaryKey(statusTask.getTaskId());
            //如果非周期性任务，修改任务列表状态
            if (handlerTask.getTaskType() != TaskTypeEnum.PERIOD.getCode()) {
                handlerTask.setTaskStatus(TaskStatusEnum.FINISHED.getCode());
                handlerTaskMapper.updateByPrimaryKeySelective(handlerTask);
            }
        }finally {
            SEMAPHORE.release();
        }
    }

    @Override
    public void deleteDbById(Integer dbId) {
        int count = commitTableRecordMapper.selectCountByDbId(dbId);
        if (count > 0) {
            throw new BussinessException("当前数据库连接在提交列表中有记录，不可删除");
        }
        sysDBInfoMapper.deleteByPrimaryKey(dbId);
    }


    /**
     * 转换为任务周期
     *
     * @param vo
     * @return
     */
    private String convertTaskPeriodStr(TaskDateTimeVo vo) {
        StringBuffer builder = new StringBuffer();
        if (vo.getMonths() != null && vo.getMonths() > 0) {
            if (vo.getFromBeginNum() != null && vo.getFromBeginNum() > 0) {
                builder.append("从【").append(vo.getFromBeginNum()).append("】月开始 ");
            }
            builder.append("每隔【").append(vo.getMonths()).append("】月");
        } else if (vo.getDays() != null && vo.getDays() > 0) {
            if (vo.getFromBeginNum() != null && vo.getFromBeginNum() > 0) {
                builder.append("从【").append(vo.getFromBeginNum()).append("】号开始 ");
            }
            builder.append("每隔【").append(vo.getDays()).append("】天");
        } else if (vo.getHour() != null && vo.getHour() > 0) {
            if (vo.getFromBeginNum() != null && vo.getFromBeginNum() >= 0) {
                builder.append("从【").append(vo.getFromBeginNum()).append("】时开始 ");
            }
            builder.append("每隔【").append(vo.getHour()).append("】小时");
        } else if (vo.getMinute() != null && vo.getMinute() > 0) {
            if (vo.getFromBeginNum() != null && vo.getFromBeginNum() >= 0) {
                builder.append("从【").append(vo.getFromBeginNum()).append("】分开始 ");
            }
            builder.append("每隔【").append(vo.getMinute()).append("】分钟");
        } else {
            throw new RuntimeException("时间格式有误");
        }

        builder.append("执行一次");

        return builder.toString();
    }

    /**
     * 转换
     *
     * @param
     * @return
     * @author silent
     * @date 2019/2/20
     */
    private String convertTaskDateTimeVoToCron(TaskDateTimeVo vo) {
        if (null == vo) {
            return null;
        }
        String pattern = "0 {0} {1} {2} {3} ? *";
        String monthString = "*";
        String daysString = "*";
        String hourString = "*";
        String minuteString = "*";
        Integer fromBeginNum;
        if (vo.getMonths() != null && vo.getMonths() > 0) {
            fromBeginNum = vo.getFromBeginNum() == null || vo.getFromBeginNum() == 0 ? 1 : vo.getFromBeginNum();
            monthString = monthString.replace("*", fromBeginNum + "/" + vo.getMonths());
            daysString = "0";
            hourString = "0";
            minuteString = "0";
        } else if (vo.getDays() != null && vo.getDays() > 0) {
            fromBeginNum = vo.getFromBeginNum() == null || vo.getFromBeginNum() == 0 ? 1 : vo.getFromBeginNum();
            daysString = daysString.replace("*", fromBeginNum + "/" + vo.getDays());
            hourString = "0";
            minuteString = "0";
        } else if (vo.getHour() != null && vo.getHour() > 0) {
            fromBeginNum = vo.getFromBeginNum() == null ? 0 : vo.getFromBeginNum();
            hourString = hourString.replace("*", fromBeginNum + "/" + vo.getHour());
            minuteString = "0";
        } else if (vo.getMinute() != null && vo.getMinute() > 0) {
            fromBeginNum = vo.getFromBeginNum() == null ? 0 : vo.getFromBeginNum();
            minuteString = minuteString.replace("*", fromBeginNum + "/" + vo.getMinute());
        } else {
            throw new RuntimeException("时间格式有误");
        }
        return MessageFormat.format(pattern, minuteString, hourString, daysString, monthString);
    }


}
