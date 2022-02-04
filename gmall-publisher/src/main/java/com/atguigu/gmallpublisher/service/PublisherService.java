package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {
    //返回日活总数数据
    public Integer getDauTotal(String date);

    //返回分时数据
    public Map<String,Long> getDauHourTotal(String date);

    //返回交易额总数数据
    public Double getGmvTotal(String date);

    //返回分时交易额数据
    public Map<String,Double> getGmvTotalHour(String date);

    //返回灵活分析需求的数据
    public Map<String,Object> getSaleDetail(String date,Integer startpage,Integer size,String keyword) throws IOException;

}
