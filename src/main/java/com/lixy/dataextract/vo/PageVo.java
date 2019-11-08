package com.lixy.dataextract.vo;

import java.io.Serializable;
import java.util.Optional;

/**
 * Author：MR LIS，2019/10/21
 * Copyright(C) 2019 All rights reserved.
 */
public class PageVo implements Serializable {


    private Integer pageSize;

    private Integer pageIndex;

    public Integer getPageSize() {
        return Optional.ofNullable(this.pageSize).orElse(10);
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getPageIndex() {
        return Optional.ofNullable(this.pageIndex).orElse(1);
    }

    public void setPageIndex(Integer pageIndex) {
        this.pageIndex = pageIndex;
    }
}
