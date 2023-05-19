package com.peterjxl.es;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.net.InetAddress;

public class ElasticsearchClientTest {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        // 1. 创建一个Settings对象，相当于是一个配置地对象，主要配置集群的名称。
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();

        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301 ))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302 ))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303 ));
    }

    @After
    public void close() {
        client.close();
    }

    @Test
    public void createIndex() throws Exception {
        // 1. 创建一个Settings对象，相当于是一个配置地对象，主要配置集群的名称。
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();

        // 2. 创建一个客户端Client对象，相当于是一个连接。
        TransportClient client = new PreBuiltTransportClient(settings);
        // 除了配置对象，还需要配置连接的地址。注意我们不是使用HTTP形式，用的是TCP形式，所以使用的是TransportClient对象，并且端口是9301。
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301 ));
        // 为了保证集群的高可用，我们需要配置多个节点。
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302 ));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303 ));


        // 3. 使用Client对象创建一个索引库。admin()是管理的意思，indices()是索引的意思，prepareCreate()是创建的意思，最后get()是执行。
        client.admin().indices().prepareCreate("index_hello").get();
    }

    @Test
    public void setIndexMappings() throws Exception {
        // 1. 创建一个Settings对象，相当于是一个配置地对象，主要配置集群的名称。
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();

        // 2. 创建一个客户端Client对象，相当于是一个连接。
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301 ))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302 ))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303 ));

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()  // startObject()相当于是一个左大括号 {
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "long")  //field()相当于是一个键值对，这里的意思是id的类型是long
                                .field("store", true)
                            .endObject()
                            .startObject("title")
                                .field("type", "text")
                                .field("store", true)
                                .field("analyzer", "ik_smart")
                            .endObject()
                            .startObject("content")
                                .field("type", "text")
                                .field("store", true)
                                .field("analyzer", "ik_smart")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject(); // endObject()相当于是一个右大括号 }

        client.admin().indices().preparePutMapping("index_hello") // preparePutMapping()相当于是一个PUT请求
                .setType("article")
                .setSource(xContentBuilder) // 设置mapping信息，可以是XContentBuilder对象，可以是 JSON 格式的字符串
                .get(); // 执行操作
    }

    @Test
    public void testAddDocument() throws Exception{
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id", 3L)
                    .field("title", "关于梦想")
                    .field("content", "编程对我而言，就像是一颗小小的，微弱的希望的种子，我甚至都不愿意让人看见它。生怕有人看见了便要嘲讽它，它太脆弱了，经不起别人的质疑")
                .endObject();

//        client.prepareIndex("index_hello", "article", "1")
        client.prepareIndex()
                .setIndex("index_hello")    //设置索引名称
                .setType("article") //设置type
                .setId("3")         //设置文档的id，如果不设置的话，会自动生成一个
                .setSource(builder) //设置文档信息
                .get();
    }

    @Test
    public void testAddDocument2() throws Exception {
        // 创建一个Article对象，设置对象的属性
        Article article = new Article();
        article.setId(4L);
        article.setTitle("关于买房");
        article.setContent("虚假的挥霍：花几千块钱去看看海。真正的挥霍：花100万交个首付买一个名义面积70平，实际面积55平，再还30年贷款的小户型住房");

        // 把Article对象转换为JSON格式的字符串
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonDocument = objectMapper.writeValueAsString(article);
        System.out.println(jsonDocument);

        // 把文档写入索引库
        client.prepareIndex("index_hello", "article", article.getId().toString())
                .setSource(jsonDocument, XContentType.JSON)
                .get();
    }

    @Test
    public void testAddDocument3() throws Exception {
        for (int i = 5; i < 100; i++){
            // 创建一个Article对象，设置对象的属性
            Article article = new Article();
            article.setId((long) i);
            article.setTitle("关于生活" + i);
            article.setContent("1对夫妻2个打工人带3个孩子养4个父母月薪5千掏空6个钱包7天无休8十万买的房子不到9十平米生活10分困难" + i);

            // 把Article对象转换为JSON格式的字符串
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonDocument = objectMapper.writeValueAsString(article);

            // 把文档写入索引库
            client.prepareIndex("index_hello", "article", article.getId().toString())
                    .setSource(jsonDocument, XContentType.JSON)
                    .get();
        }
    }


}
