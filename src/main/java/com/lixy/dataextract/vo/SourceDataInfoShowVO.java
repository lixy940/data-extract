package com.lixy.dataextract.vo;

/**
 * Created by Guopengzhan on 2018/5/24.
 *
 * 数据展示
 */
public class SourceDataInfoShowVO {

    /**
     *数据源基本信息
     */
    private SourceDataInfoVO sourceDataInfoVO;

    /**
     * 行数
     */
    private  Long count;


    public SourceDataInfoVO getSourceDataInfoVO() {
        return sourceDataInfoVO;
    }

    public void setSourceDataInfoVO(SourceDataInfoVO sourceDataInfoVO) {
        this.sourceDataInfoVO = sourceDataInfoVO;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

}
