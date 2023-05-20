package com.peterjxl;

import com.peterjxl.es.entity.Article;
import com.peterjxl.es.repositories.ArticleRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class SpringDataESTest {

    @Autowired
    private ArticleRepository articleRepository;

    @Autowired
    private ElasticsearchTemplate template;

    @Test
    public void createIndex() throws Exception{
        // 创建索引，并配置映射关系
        template.createIndex(Article.class);
        // 如果仅仅是配置映射关系，使用： template.putMapping(Article.class);
    }

    @Test
    public void addDocument() throws Exception{
        Article article = new Article();

        for (int i = 10; i < 20; i++) {
            article.setId(i);
            article.setTitle("测试查询" + i);
            article.setContent("测试查询的内容" + i);
            articleRepository.save(article);
        }
    }

    @Test
    public void deleteDocument() {
        articleRepository.deleteById(1L);
    }

    @Test
    public void findAll() {
        Iterable<Article> articles = articleRepository.findAll();
        for (Article article : articles) {
            System.out.println(article);
        }
    }

    @Test
    public void findById() {
        Article article = articleRepository.findById(3L).get();
        System.out.println(article);
    }

    @Test
    public void testFindByTitle() {
        List<Article> articles = articleRepository.findByTitle("测试查询");
        articles.stream().forEach(System.out::println);
    }

    @Test
    public void testFindByTitleOrContent() {
        List<Article> articles = articleRepository.findByTitleOrContent("测试查询", "编程");
        articles.stream().forEach(System.out::println);
    }

    @Test
    public void testFindByTitleOrContentPage() {
        Pageable pageable = PageRequest.of(0, 5);
        List<Article> articles = articleRepository.findByTitleOrContent("测试查询", "编程", pageable);
        articles.stream().forEach(System.out::println);
    }

    @Test
    public void testNativeSearchQuery() {
        // 创建一个查询对象
        NativeSearchQuery query = new NativeSearchQueryBuilder()
                // 添加基本的分词条件
                .withQuery(QueryBuilders.matchQuery("title", "测试查询天气之子"))
                .build();
        // 执行查询
        List<Article> articles = template.queryForList(query, Article.class);
        articles.stream().forEach(System.out::println);
    }
}
