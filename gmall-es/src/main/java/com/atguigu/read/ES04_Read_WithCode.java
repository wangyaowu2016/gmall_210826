package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ES04_Read_WithCode {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接参数/属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.查询ES中的数据
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex","男");

        boolQueryBuilder.filter(termQueryBuilder);

        sourceBuilder.query(boolQueryBuilder);

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo","羽毛球");

        boolQueryBuilder.must(matchQueryBuilder);

        Search search = new Search.Builder(sourceBuilder.toString()).addType("_doc")
                .addIndex("student")
                .build();
        SearchResult result = jestClient.execute(search);

        //获取命中总数
        System.out.println("命中条数:"+result.getTotal());

        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);

        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:"+hit.index);
            System.out.println("type:"+hit.type);
            System.out.println("id:"+hit.id);
            System.out.println("score:"+hit.score);
            //获取数据明细
            Map source = hit.source;

            Set entrySet = source.entrySet();

/*            for (Object map : entrySet) {
                System.out.println(map.toString());
            }*/

            Set keySet = source.keySet();
            for (Object key : keySet) {
                System.out.println(key+":"+source.get(key));
            }

            //获取聚合数组
            MetricAggregation aggregations = result.getAggregations();
            TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");

            List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {
                System.out.println("Key:"+bucket.getKey());
                System.out.println("doc_count:"+bucket.getCount());
                System.out.println("KeyAsString:"+bucket.getKeyAsString());

                //获取年龄聚合组数据
                MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
                System.out.println("value:"+groupByAge.getMax());

            }
        }

        //关闭连接
        jestClient.shutdownClient();
    }
}
