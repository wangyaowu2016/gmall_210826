package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import com.atguigu.gmallpublisher.service.impl.PublisherServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {

        //1.获取日活总数
        Integer dauTotal = publisherService.getDauTotal(date);
        //获取交易额总数
        Double gmvTotal = publisherService.getGmvTotal(date);

        //2.创建存放新增日活的Map集合
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //3.创建存放新增设备的Map集合
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        //4.创建存放新增交易额的Map集合
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", gmvTotal);

        //4.将Map集合封装到List集合中
        ArrayList<Map> result = new ArrayList<>();
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHour(
            @RequestParam("id") String id,
            @RequestParam("date") String date) {
        //1.根据今天的日期获取昨天的日期
        LocalDate yesterday = LocalDate.parse(date).plusDays(-1);

        Map todayMap = null;//类型自动推断
        Map yesterdayMap = null;//类型自动推断

        if ("dau".equals(id)) {
            //2.根据日期获取Service获取处理后的数据
            todayMap = publisherService.getDauHourTotal(date);

            yesterdayMap = publisherService.getDauHourTotal(yesterday.toString());
        } else if ("order_anmount".equals(id)) {
            //交易额现关数据
            todayMap = publisherService.getGmvTotalHour(date);

            yesterdayMap = publisherService.getGmvTotalHour(yesterday.toString());

        }

        //3.创建存放最终结果的Map集合
        HashMap<String, Map> result = new HashMap<>();

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);

    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(
            @RequestParam("date") String date,
            @RequestParam("startpage") Integer startpage,
            @RequestParam("size") Integer size,
            @RequestParam("keyword") String keyword) throws IOException {
        return JSONObject.toJSONString(publisherService.getSaleDetail(date ,startpage,size,keyword));
    }

}
