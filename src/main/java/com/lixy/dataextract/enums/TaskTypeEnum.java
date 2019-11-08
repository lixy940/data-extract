package com.lixy.dataextract.enums;

import com.lixy.dataextract.vo.KeyValueVo;

import java.util.ArrayList;
import java.util.List;

/**
 * Author：MR LIS，2019/10/29
 * Copyright(C) 2019 All rights reserved.
 */
public enum TaskTypeEnum {
    ONCE(1, "永久一次"),

    PERIOD(2, "周期执行"),

//    Schedule(3, "指定时间"),
    ;

    private int code;
    private String name;

    TaskTypeEnum(int code, String name) {
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
        for (TaskTypeEnum value : TaskTypeEnum.values()) {
            if (code == value.getCode()) {
                return value.getName();
            }
        }
        return null;
    }

    public static List<KeyValueVo> getList() {
        List<KeyValueVo> list = new ArrayList<>();
        for (TaskTypeEnum value : TaskTypeEnum.values()) {
            list.add(new KeyValueVo(value.getCode(), value.getName()));
        }

        return list;
    }
}
