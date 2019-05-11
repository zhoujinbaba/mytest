package com.itheima.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itheima.es.domain.Article;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.junit.runner.Request;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * @Author: zh
 * @Date: 2019/2/27 19:54
 */
public class elasticsearchTest {
    /**
     * 创建删除索引
     * @throws Exception
     */
    @Test
    public void makeIndex() throws Exception {
        //创建Client
        TransportClient client =new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //创建索引
        //client.admin().indices().prepareCreate("blog").get();
        //删除索引
        client.admin().indices().prepareDelete("ix").get();

        client.close();


    }

    /**
     * 创建映射
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void createMapping() throws IOException, ExecutionException, InterruptedException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        //创建映射
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type","Long")
                                .field("store",false)
                                .field("index",false)
                            .endObject()
                            .startObject("title")
                                .field("type","Long")
                                .field("store",false)
                                .field("index",false)
                                .field("analyzer","ik_smart")
                            .endObject()
                            .startObject("content")
                                .field("type","Long")
                                .field("store",false)
                                .field("index",false)
                                .field("analyser","ik_smart")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        PutMappingRequest putMappingRequest = Requests.putMappingRequest("ix2").type("article").source(xContentBuilder);
        client.admin().indices().putMapping(putMappingRequest).get();

        client.close();


    }

    /**
     * 创建文档
     *
     * @throws UnknownHostException
     */
    @Test
    public void createDocuments() throws IOException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        //创建文档
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id",1)
                .field("title","ElasticSearch是一个基于Lucene的搜索服务器123")
                .field("content","它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是\n" +
                        "用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能\n" +
                                "够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject();

        //添加文档到指定索引库
        client.prepareIndex("ix2","article","1").setSource(builder).get();
        client.close();


    }

    /**
     * 创建文档
     * json
     */
    @Test
    public void createDocuments_2() throws UnknownHostException, JsonProcessingException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        final Article article = new Article();
        article.setId(3L);
        article.setTitle("长恨人心不如水，等闲平地起波澜");
        article.setContent("好诗好诗");
        //转换成json

        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(article);
        System.out.println(jsonString);

        //创建文档
        IndexResponse indexResponse = client.prepareIndex("ix2", "article", "3").setSource(jsonString, XContentType.JSON).get();

        System.out.println(indexResponse.getId());

        client.close();

    }

    @Test
    public void deleteDocuments() throws UnknownHostException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        client.prepareDelete("ix2","article","3").get();

        client.close();


    }

    /**
     * d第二种
     * @throws UnknownHostException
     */

    @Test
    public void deleteDocuments_2() throws UnknownHostException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)

                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        client.delete(new DeleteRequest("ix2","com","1"));
        client.close();


    }

    /**
     * 字符串查询
     * @throws Exception
     */
    @Test
    public void findString ()throws Exception{
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)

                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //设置查询条件
        SearchResponse searchResponse = client.prepareSearch("ix2").setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("基于")).get();
        SearchHits hits = searchResponse.getHits();
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()){
            SearchHit next = iterator.next();
            System.out.println(next.getSourceAsString());
        }


    }

    /**
     * 词条查询
     * @throws Exception
     */
    @Test
    public  void queryForWords() throws  Exception{
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)

                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        //设置查询条件
        SearchResponse searchResponse = client.prepareSearch("ix2").setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "搜索")).get();
        SearchHits hits = searchResponse.getHits();
        Iterator<SearchHit> iterator = hits.iterator();
        while(iterator.hasNext()){
            SearchHit next = iterator.next();

            System.out.println(next.getSourceAsString());

        }


    }

    /**
     * 添加100条文档
     * @throws UnknownHostException
     */
    @Test
    public void addMore() throws UnknownHostException, JsonProcessingException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)

                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        /*Article article = new Article();
        for (int i = 0; i < 100; i++) {


            article.setId((long) i);
            article.setTitle("有道词典");
            article.setContent("完整收录《21世纪大英汉词典》及《新汉英大辞典》，离线也能查单词！\n" +
                    "同时支持在线翻译、网络释义、屏幕取词、海量例句、全文翻译等功能。有道词典本地" +
                    "增强版介绍有道词典本地增强版――本" +
                    "地词库大扩容，完整收录《21世纪大英汉词典》及《新汉英大辞典》" +
                    "，权威词典随身带，离线在线样样行！离线状态，使用内置本地词库，解释权威丰富" +
                    "！在线使用，通过网络释义功能，搞定新词怪词！海量例句，与互联网同步更新，中英对照参考！" +
                    "在线翻译，接驳有道全文翻译，双语轻松互译");
            ObjectMapper mapper = new ObjectMapper();
            String string = mapper.writeValueAsString(article);


            IndexResponse indexResponse = client.prepareIndex("blog", "article", article.getId().toString()).setSource(string, XContentType.JSON).get();
*/
            SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                    .setQuery(QueryBuilders.matchAllQuery()).get();
            SearchHits hits = searchResponse.getHits();
            Iterator<SearchHit> iterator = hits.iterator();
            while(iterator.hasNext()) {
                SearchHit next = iterator.next();
                System.out.println(next.getSourceAsString());



        }
    }



}
