package com.lixy.dataextract.vo;

/**
 * Author：MR LIS，2019/10/23
 * Copyright(C) 2019 All rights reserved.
 */
public class KeyValueVo {

    private int code;

    private String name;

    public KeyValueVo() {
    }

    public KeyValueVo(int code, String name) {
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
}
