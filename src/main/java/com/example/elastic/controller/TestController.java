package com.example.elastic.controller;

import com.alibaba.fastjson.JSON;
import com.example.elastic.pojo.User;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.w3c.dom.ls.LSOutput;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@RestController
public class TestController {
    @Resource
    private RestHighLevelClient client;

    @GetMapping("/add/{name}/{age}")
    public boolean test(@PathVariable String name,@PathVariable int age) throws IOException {
        User user = new User(name,age);
        IndexRequest indexRequest = new IndexRequest("test5")
                .id("4")
                .source("user", user.getName(),
                        "age", user.getAge());
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        return true;
    }

    @GetMapping("/search/{key}/{currentPage}/{pageSize}")
    public List<Map<String, Object>> search(@PathVariable String key, @PathVariable int currentPage, @PathVariable int pageSize) throws IOException {
        SearchRequest searchRequest = new SearchRequest("test5");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //分页,currentPage从0开始
        searchSourceBuilder.from(currentPage);
        searchSourceBuilder.size(pageSize);

        //精准匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("user", key);
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        //执行搜索
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(Arrays.toString(response.getHits().getHits()));

        //解析结果
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        for (SearchHit hit : response.getHits().getHits()) {
            list.add(hit.getSourceAsMap());
            System.out.println(list);
        }
        return list;
    }
}
