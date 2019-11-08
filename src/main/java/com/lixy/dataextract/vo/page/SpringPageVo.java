package com.lixy.dataextract.vo.page;

import com.github.pagehelper.PageInfo;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * @author yuanyang
 * @Description:分页对象
 * @date 2018/3/6 16:24
 */
public class SpringPageVo<T> implements Serializable {

    private static final long serialVersionUID = -6344449330097328246L;

    private List<T> result = Collections.emptyList();

    /**
     * 总条数
     */
    private Long total = 0L;

    /**
     * 每页多少条数据
     */
    private Integer pageSize = 20;

    /**
     * 当前页
     */
    private Integer pageIndex = 0;


    public SpringPageVo() {
    }


    /**
     * 无需页码的分页
     *
     * @return
     * @author silent
     * @date 2019/8/12
     */
    public SpringPageVo(Long total, List<T> result) {
        this.total = total;
        this.result = result;
        this.pageIndex = null;
        this.pageSize = null;
    }


    /**
     * 正常分页
     *
     * @return
     * @author silent
     * @date 2019/8/12
     */
    public SpringPageVo(Long total, List<T> result, Integer pageIndex, Integer pageSize) {
        this.total = total;
        this.result = result;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }


    /**
     * 带pageInfo的构造函数
     *
     * @return
     * @author silent
     * @date 2019/8/12
     */
    public SpringPageVo(PageInfo<T> pageInfo, Integer pageIndex, Integer pageSize) {
        this.total = pageInfo.getTotal();
        this.result = pageInfo.getList();
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }


    /**
     * 总页数
     */
    public Integer getTotalPages() {
        if (getPageSize() == null) {
            return null;
        }
        return getPageSize() == 0 ? 1 : (int) Math.ceil((double) total / (double) getPageSize());
    }


    /**
     * 当前返回的数据条数
     */
    public Integer getNumberOfElements() {
        return result.size();
    }


    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        SpringPageVo<T> vo = new SpringPageVo<>();

        public Builder<T> result(List<T> content) {
            vo.setResult(content);
            return this;
        }

        public Builder<T> total(Long totalElements) {
            vo.setTotal(totalElements);
            return this;
        }

        public Builder<T> size(Integer size) {
            vo.setPageSize(size);
            return this;
        }

        public Builder<T> number(Integer number) {
            vo.setPageIndex(number);
            return this;
        }

        public SpringPageVo<T> build() {
            return vo;
        }
    }


    public List<T> getResult() {
        return result;
    }

    public void setResult(List<T> result) {
        this.result = result;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long totalElements) {
        this.total = totalElements;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(Integer pageIndex) {
        this.pageIndex = pageIndex;
    }
}
