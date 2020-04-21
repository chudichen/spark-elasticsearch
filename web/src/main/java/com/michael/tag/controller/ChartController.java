package com.michael.tag.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 表格控制器
 *
 * @author chudichen
 * @since 2020/4/21
 */
@Slf4j
@Controller
public class ChartController {

    @RequestMapping("/tags")
    public String tags() {
        return "tags";
    }

    @RequestMapping("/")
    public String index() {
        return "index";
    }
}
