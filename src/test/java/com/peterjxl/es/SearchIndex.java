package com.peterjxl.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class SearchIndex {
    private TransportClient client;

    @Before
    public void init() throws Exception{
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
    }

    @After
    public void close() {
        client.close();
    }

    @Test
    public void searchIndexWithID() {
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("3", "4");
        search(queryBuilder);
    }


    @Test
    public void searchIndexWithTerm() {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title", "梦想");
        search(queryBuilder);
    }

    @Test
    public void searchIndexWithQueryString() {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("梦想是什么").defaultField("title");
        search(queryBuilder);
    }

    @Test
    public void searchIndexWithPaging() {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("生活是什么").defaultField("title");
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .setFrom(0) // 起始记录下标
                .setSize(5) // 每页显示的记录数
                .get();
        SearchHits searchHits = searchResponse.getHits();
        System.out.println("查询结果总记录数" + searchHits.getTotalHits());

        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
            System.out.println("文档的属性: ");
            Map<String, Object> document = searchHit.getSource();
            System.out.println("id:" + document.get("id"));
            System.out.println("title:" + document.get("title"));
            System.out.println("content:" + document.get("content"));
            System.out.println("-------------------------- ");
        }
    }

    @Test
    public void searchIndexWithHighLight() {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");

        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("梦想").defaultField("title");
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .highlighter(highlightBuilder)
                .get();
        SearchHits searchHits = searchResponse.getHits();
        System.out.println("查询结果总记录数：" + searchHits.getTotalHits());
        for (SearchHit searchHit : searchHits) {
            System.out.println("高亮结果: ");
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println(highlightFields);

            // 遍历高亮字段
            HighlightField highlightField = highlightFields.get("title");
            Text[] fragments = highlightField.getFragments();
            if(fragments != null){
                String title = fragments[0].toString();
                System.out.println("title: " + title);
            }
        }
    }

    public void search(QueryBuilder queryBuilder){
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .get();
        SearchHits searchHits = searchResponse.getHits();
        System.out.println("查询结果总记录数" + searchHits.getTotalHits());

        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
            System.out.println("文档的属性: ");
            Map<String, Object> document = searchHit.getSource();
            System.out.println("id:" + document.get("id"));
            System.out.println("title:" + document.get("title"));
            System.out.println("content:" + document.get("content"));
            System.out.println("-------------------------- ");
        }
    }

}
