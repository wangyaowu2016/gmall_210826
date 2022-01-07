package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class ES01_BulkWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂类
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.设置连接参数
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.通过客户端工厂获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.批量写入
        Movie movie103 = new Movie("103","特斯拉大战金刚1");
        Movie movie104 = new Movie("104","特斯拉大战金刚2");
        Movie movie105 = new Movie("105","特斯拉大战金刚3");

        Index index103 = new Index.Builder(movie103).id("1003").build();
        Index index104 = new Index.Builder(movie104).id("1004").build();
        Index index105 = new Index.Builder(movie105).id("1005").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie_")
                .defaultType("_doc")
                .addAction(index103)
                .addAction(index104)
                .addAction(index105)
                .build();

        jestClient.execute(bulk);

        //关闭连接
        jestClient.shutdownClient();
    }
}
