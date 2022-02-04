package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

// @Controller 定义在类上，作用:将这个类标识为Controller层 默认返回的是一个页面对象
//@Controller+@ResponseBody=@RestController
@RestController // @RestController 定义在类上，作用等价于@Controller+@ResponseBody，既能标识为Controller层又可以返回一个普通对象
public class TestController {

    @RequestMapping("test") // @RequestMapping("xxx")定义在方法上，作用：通过映射名来判断调取的方法是哪一个
//    @ResponseBody 定义在方法上，作用：可以返回普通的对象，比如String
    public String test1() {
        System.out.println("test");
        return "success";
    }

    @RequestMapping("test1")
//    @RequestParam("xxx") 定义在形参列表上，作用：通过映射名来判断请求携带的参数，传给哪个形参
    public String test2(@RequestParam("name") String name,@RequestParam("age") Integer age) {
        System.out.printf("aaa");
        return "name:"+name+","+"age:"+age;
    }

}
