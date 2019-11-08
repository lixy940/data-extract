package com.lixy.dataextract.constants;

import com.lixy.dataextract.utils.DBPropertyReaderUtils;
import com.lixy.dataextract.utils.IPUtil;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public final class StaticParameterUtils {


    /**
     * token header
     */
    public static final String HTTP_HEADER_TOKEN = "";

    /**
     * local pg db ID of storage data
     */
    public static final Integer LOCAL_STORAGE_PG_DB_ID = Integer.valueOf(DBPropertyReaderUtils.getProValue("local.storage.pg.db.id"));
    /**
     * 并发信号量
     */
    public static final Integer CONCURRENT_SEMAPHORE_COUNT = Integer.valueOf(DBPropertyReaderUtils.getProValue("concurrent.semaphore.count"));
    /**
     * etl server path
     */
    public static final String ETL_BASE_SERVER_PATH = DBPropertyReaderUtils.getProValue("etl.base.server.path");


    public static final String SERVER_PORT = DBPropertyReaderUtils.getProValue("server.port");

    /**
     * 更新任务导入状态
     */
    public static  final String CALL_BACK_URL = "http://" + IPUtil.getInnetIp() + ":" + StaticParameterUtils.SERVER_PORT + "/remoterequset/updateTaskStatus";


}
