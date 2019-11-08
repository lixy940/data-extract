package com.lixy.dataextract.enums;

import com.lixy.dataextract.vo.KeyValueVo;

import java.util.ArrayList;
import java.util.List;

/**
 * Author：MR LIS，2019/10/23
 * Copyright(C) 2019 All rights reserved.
 */
public enum TaskStatusEnum {

    PENDING(0,"未开始"),
    PROCESSING(1,"进行中"),
    FINISHED(2,"已完成"),
    ;

    private int code;

    private String name;

    TaskStatusEnum(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
    public static String getName(int code) {
        for (TaskStatusEnum value : TaskStatusEnum.values()) {
            if (code == value.getCode()) {
                return value.getName();
            }
        }
        return null;
    }

    public static List<KeyValueVo> getList() {
        List<KeyValueVo> list = new ArrayList<>();
        for (TaskStatusEnum value : TaskStatusEnum.values()) {
            list.add(new KeyValueVo(value.getCode(), value.getName()));
        }

        return list;
    }

}
