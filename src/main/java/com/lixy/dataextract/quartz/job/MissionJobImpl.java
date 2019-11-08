package com.lixy.dataextract.quartz.job;


import com.lixy.dataextract.constants.StaticParameterUtils;
import com.lixy.dataextract.quartz.JobConstant;
import com.lixy.dataextract.service.DataExtractService;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * 定时导入任务
 **/
@Component
@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class MissionJobImpl implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(MissionJobImpl.class);

    private final static ExecutorService THREAD_POOL = new ThreadPoolExecutor(StaticParameterUtils.CONCURRENT_SEMAPHORE_COUNT, StaticParameterUtils.CONCURRENT_SEMAPHORE_COUNT, 0, TimeUnit.MICROSECONDS, new LinkedBlockingQueue<Runnable>());

    @Autowired
    private DataExtractService dataExtractService;

    @Override
    public void execute(JobExecutionContext context) {
        try {
            JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
            Integer taskId = (Integer) jobDataMap.get(JobConstant.TASK_ID_KEY);
            THREAD_POOL.submit(() -> dataExtractService.executeTask(taskId));
        } catch (Exception e) {
            LOGGER.error("定时任务导入执行失败", e);
        }
    }
}
