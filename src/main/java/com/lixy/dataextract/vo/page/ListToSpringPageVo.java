package com.lixy.dataextract.vo.page;

import java.util.List;

/**
 * @Description:分页转换器
 * @date 2018/3/6 17:14
 */
public class ListToSpringPageVo {

    public static <T> SpringPageVo<T> listToPage(Integer pageIndex, Integer pageSize, Long total, List<T> lists){
        SpringPageVo<T> springPageVo = new SpringPageVo<>();
        springPageVo.setResult(lists);
        springPageVo.setPageIndex(pageIndex);
        springPageVo.setPageSize(pageSize);
        springPageVo.setTotal(total);
        return springPageVo;
    }
}
