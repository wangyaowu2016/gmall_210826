package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


//@Controller
@RestController
public class TestController {

    @RequestMapping("test")
//    @ResponseBody
    public String test1() {
        return "success";
    }

    @RequestMapping("test1")
    public String test2(@RequestParam("name") String name,@RequestParam("age") Integer age) {
        System.out.printf("aaa");
        return "name:"+name+","+"age:"+age;
    }

}
