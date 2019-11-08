package com.lixy.dataextract.enums;

import com.lixy.dataextract.vo.KeyValueVo;

import java.util.ArrayList;
import java.util.List;

/**
 * Author：MR LIS，2019/10/23
 * Copyright(C) 2019 All rights reserved.
 */
public enum ColumnTypeEnum {

    STRING(1, "字符串"),
    DATE(2, "日期"),
    NUMBER(3, "日期型字符串"),
    ;


    private int code;
    /**
     * 中文名称
     */
    private String name;

    ColumnTypeEnum(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static List<KeyValueVo> getList() {
        List<KeyValueVo> list = new ArrayList<>();
        for (ColumnTypeEnum value : ColumnTypeEnum.values()) {
            list.add(new KeyValueVo(value.getCode(), value.getName()));
        }

        return list;
    }


    public static String getName(int code) {
        for (ColumnTypeEnum value : ColumnTypeEnum.values()) {
            if (value.code == code) {
                return value.getName();
            }
        }
        return null;
    }
}
