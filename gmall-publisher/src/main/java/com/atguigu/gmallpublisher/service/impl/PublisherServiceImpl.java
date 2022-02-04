package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service //@Service 定义在普通的类上，作用将这个类标识为Service层
public class PublisherServiceImpl implements PublisherService {

    //@Autowired 定义在接口变量上，作用：可以自动获取这个接口的实现类
    @Autowired //自动注入获取到接口的实现
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHourTotal(String date) {
        //1.获取老的Map集合
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建新的Map集合
        HashMap<String, Long> result = new HashMap<>();

        //3.取出老Map中的数据，放入新Map集合
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getGmvTotalHour(String date) {
        //1.获取老的Map集合
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建新的Map集合
        HashMap<String, Double> reslt = new HashMap<>();

        //3.取出老Map中的数据，放入新Map集合
        for (Map map : list) {
            reslt.put((String) map.get("CREATE_HOUR"), (Double)map.get("SUM_AMOUNT") );
        }

        return reslt;
    }

    @Override
    public Map<String, Object> getSaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_DETAIL_INDEXNAME+"0826").addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        //TODO 1.获取数据条数
        Long total = searchResult.getTotal();

        //TODO 2.获取明细数据
        ArrayList<Map> details = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        //TODO 3.获取年龄聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation groupbyUserAge = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets = groupbyUserAge.getBuckets();
/*      List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_user_age").getBuckets();*/

        //定义变量用来保存20岁以下的个数
        Long low20count = 0l;

        //定义变量用来保存30岁以上的个数
        Long up30count = 0l;

        for (TermsAggregation.Entry bucket : buckets) {
            if (Integer.parseInt(bucket.getKey())<20){
                low20count+=bucket.getCount();
            }
            if (Integer.parseInt(bucket.getKey())>=30){
                up30count+=bucket.getCount();
            }
        }
        //获取20岁以下的年龄占比
        double low20Ratio = Math.round(low20count * 1000D / total) / 10d;
        //获取30岁以上的年龄占比
        double up30Ratio = Math.round(up30count * 1000D / total) / 10d;
        //获取20-30岁之间的年龄占比
        double up20Low30Ratio = Math.round((100 - low20Ratio - up30Ratio) * 10D) / 10D;

        //创建20岁以下的Option对象
        Option low20Opt = new Option("20岁以下", low20Ratio);

        //创建20岁到30岁之间的的Option对象
        Option up20Low30Opt = new Option("20岁到30岁", up20Low30Ratio);

        //创建30岁以上的的Option对象
        Option up30Opt = new Option("30岁及30岁以上", up30Ratio);

        //创建存放Option对象的list集合
        ArrayList<Option> ageOptList = new ArrayList<>();
        ageOptList.add(low20Opt);
        ageOptList.add(up20Low30Opt);
        ageOptList.add(up30Opt);

        //创建年龄占比的Stat对象
        Stat ageStat = new Stat("用户年龄占比", ageOptList);

        //TODO 4.获取性别聚合组数据
        TermsAggregation groupbyUserGender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> genderBuckets = groupbyUserGender.getBuckets();
        //创建存放男生的个数
        Long maleCount = 0l;

        for (TermsAggregation.Entry bucket : genderBuckets) {
            if ("M".equals(bucket.getKey())) {
                maleCount += bucket.getCount();
            }
        }

        //获取男生性别占比
        double maleRatio = Math.round(maleCount * 1000d / total) / 10d;
        //获取女生性别占比
        double femaleRatio = Math.round((100 - maleRatio) * 10d) / 10d;

        //创建存放男生占比的option对象
        Option maleOpt = new Option("男",maleRatio);
        //创建存放女生占比的option对象
        Option femaleOpt = new Option("女",femaleRatio);

        //创建性别现关Option对象的的Liat集合
        ArrayList<Option> genderOptList = new ArrayList<>();
        genderOptList.add(maleOpt);
        genderOptList.add(femaleOpt);

        //创建性别占比的Stat对象
        Stat genderStat = new Stat("用户性别占比", genderOptList);

        //创建存放Stat对象的list集合
        List<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //TODO 5.创建存放最终结果的Map集合
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat",stats);
        result.put("detail",details);

        return result;
    }
}
