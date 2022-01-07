package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class ES01_SingleWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接参数
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.往ES中写入数据
/*        Index index1 = new Index.Builder("{\n" +
                "  \"id\":\"103\",\n" +
                "  \"name\":\"反贪风暴5\"\n" +
                "}").index("movie1").type("_doc").id("1003").build();
        jestClient.execute(index1);
        */

        Movie movie = new Movie("102","驴得水");
        Index index = new Index.Builder(movie)
                .index("movie")
                .type("_doc")
                .id("1002")
                .build();
        jestClient.execute(index);

        //关闭连接
        jestClient.shutdownClient();
    }
}
