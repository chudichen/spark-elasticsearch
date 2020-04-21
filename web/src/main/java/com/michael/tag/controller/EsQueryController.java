package com.michael.tag.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletResponse;

/**
 * @author chudichen
 * @since 2020/4/21
 */
@Controller
public class EsQueryController {


    @RequestMapping("gen")
    public void genAndDown(HttpServletResponse response, @RequestBody String data) {
        JSONObject object = JSON.parseObject(data);
        JSONArray selectedTags = object.getJSONArray("selectedTags");
    }
}
