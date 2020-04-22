package com.michael.tag.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.michael.tag.entity.ESTag;
import com.michael.tag.entity.MemberTag;
import com.michael.tag.service.ESQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

/**
 * @author chudichen
 * @since 2020/4/21
 */
@Controller
public class EsQueryController {

    private final ESQueryService service;

    public EsQueryController(ESQueryService service) {
        this.service = service;
    }

    @RequestMapping("/gen")
    public void genAndDown(HttpServletResponse response, @RequestBody String data) {
        JSONObject object = JSON.parseObject(data);
        JSONArray selectedTags = object.getJSONArray("selectedTags");
        List<ESTag> list = selectedTags.toJavaList(ESTag.class);
        List<MemberTag> tags = service.buildQuery(list);
        String content = toContent(tags);
        String fileName = "member.txt";
        response.setContentType("application/octet-stream");

        try {
            response.setHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(fileName, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        ServletOutputStream sos = null;
        BufferedOutputStream bos = null;

        try {
            sos = response.getOutputStream();
            bos = new BufferedOutputStream(sos);
            bos.write(content.getBytes("UTF-8"));
            bos.flush();
            bos.close();
            sos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @RequestMapping("/down")
    public void down(HttpServletResponse response) {
        String fileName = "member.txt";
        response.setContentType("text/plain");

        try {
            response.setHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(fileName, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        ServletOutputStream sos;
        BufferedOutputStream bos;

        try {
            sos = response.getOutputStream();
            bos = new BufferedOutputStream(sos);
            bos.write("content".getBytes("UTF-8"));
            bos.flush();
            bos.close();
            sos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private String toContent(List<MemberTag> tags) {
        StringBuilder sb = new StringBuilder();
        for (MemberTag tag : tags) {
            sb.append("[").append(tag.getMemberId()).append(",").append(tag.getPhone()).append("]\r\n");
        }

        return sb.toString();
    }
}
