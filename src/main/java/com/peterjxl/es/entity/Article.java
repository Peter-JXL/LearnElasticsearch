package com.peterjxl.es.entity;


import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "spring-data-es-blog", type = "article")
public class Article {
    @Id
    @Field(type = FieldType.Long, store = true)
    private long id;

    // 默认会索引
    @Field(type = FieldType.text, store = true, analyzer = "ik_smart")
    private String title;

    @Field(type = FieldType.text, store = true, analyzer = "ik_smart")
    private String content;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }


    public void setContent(String content) {


        this.content = content;
    }

    @Override
    public String toString() {
        return "Article{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", content='" + content + '\'' +
                '}';
    }

}
