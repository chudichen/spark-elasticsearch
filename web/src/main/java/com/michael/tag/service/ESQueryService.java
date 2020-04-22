package com.michael.tag.service;

import com.alibaba.fastjson.JSON;
import com.michael.tag.entity.ESTag;
import com.michael.tag.entity.MemberTag;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chudichen
 * @since 2020/4/21
 */
@Slf4j
@Service
public class ESQueryService {

    @Resource(name = "highLevelClient")
    private RestHighLevelClient highLevelClient;

    public List<MemberTag> buildQuery(List<ESTag> tags) {
        SearchRequest request = new SearchRequest();
        request.indices("tag");
        request.types("_doc");
        String[] includes = {"memberId", "phone"};
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        builder.query(boolQueryBuilder);
        builder.from(0);
        builder.size(1000);
        builder.fetchSource(includes, null);

        List<QueryBuilder> should = boolQueryBuilder.should();
        List<QueryBuilder> mustNot = boolQueryBuilder.mustNot();
        List<QueryBuilder> must = boolQueryBuilder.must();

        for (ESTag tag : tags) {
            String name = tag.getName();
            String value = tag.getValue();
            String type = tag.getType();

            switch (type) {
                case "match":
                    should.add(QueryBuilders.matchQuery(name, value));
                    break;
                case "notMatch":
                    mustNot.add(QueryBuilders.matchQuery(name, value));
                case "rangeBoth":
                    String[] split = value.split("-");
                    String v1 = split[0];
                    String v2 = split[1];
                    should.add(QueryBuilders.rangeQuery(name).lte(v2).gte(v1));
                case "rangeGte":
                    should.add(QueryBuilders.rangeQuery(name).gte(value));
                case "rangeLte":
                    should.add(QueryBuilders.rangeQuery(name).lte(value));
                case "exists":
                    should.add(QueryBuilders.existsQuery(name));
                default:
            }

            log.info("DSL: {}", boolQueryBuilder.toString());
            request.source(builder);
            RequestOptions options = RequestOptions.DEFAULT;
            List<MemberTag> memberTags = new ArrayList<>();
            try {
                SearchResponse search = highLevelClient.search(request, options);
                SearchHits hits = search.getHits();
                for (SearchHit next : hits) {
                    String sourceAsString = next.getSourceAsString();
                    MemberTag memberTag = JSON.parseObject(sourceAsString, MemberTag.class);
                    memberTags.add(memberTag);
                }
                return memberTags;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
