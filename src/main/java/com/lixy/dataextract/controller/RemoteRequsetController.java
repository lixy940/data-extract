package com.lixy.dataextract.controller;

import com.lixy.dataextract.service.DataExtractService;
import com.lixy.dataextract.vo.ResponseResult;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Author：MR LIS，2019/10/24
 * Copyright(C) 2019 All rights reserved.
 */
@Api(tags = {"远程请求服务"})
@RestController
@RequestMapping("/remoterequset")
public class RemoteRequsetController {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private DataExtractService dataExtractService;

    @ApiOperation(value = "更新任务状态", notes = "更新任务状态")
    @PostMapping("/updateTaskStatus")
    public ResponseResult updateTaskStatus(@RequestParam Integer handlerId, @RequestParam String beginIndex, @RequestParam String endIndex) {
        logger.info("request param :handlerId--{},---{},---{}", handlerId, beginIndex, endIndex);
        ResponseResult result = new ResponseResult();
        dataExtractService.updateTaskStatus(handlerId, beginIndex, endIndex);
        return result;
    }

}
