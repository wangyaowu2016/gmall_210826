package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //查询总数数据抽象方法
    public Integer selectDauTotal(String Date);

    //查询分时数据的抽象方法
    public List<Map> selectDauTotalHourMap(String date);
}
