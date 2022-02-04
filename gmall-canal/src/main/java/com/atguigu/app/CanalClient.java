package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

import static com.atguigu.constants.GmallConstants.*;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.创建Cancal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102",11111), "example", "canal", "canal");

        while (true) {
            //2.连接Canal
            canalConnector.connect();

            //3.订阅要获取数据的数据库
            canalConnector.subscribe("gmall210826.*");

            //4.获取多个sql的执行结果
            Message message = canalConnector.get(100);

            //5.获取每一个sql执行的结果
            List<CanalEntry.Entry> entries = message.getEntries();

            if (entries.size()<=0){
                System.out.println("没有数据，休息一会儿");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                //6.遍历存放entry的list集合取出每一个entry
                for (CanalEntry.Entry entry : entries) {
                    //TODO 获取TableName
                    String tableName = entry.getHeader().getTableName();

                    //8.获取Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //9.根据Entry类型判断，再获取序列化的数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        ByteString storeValue = entry.getStoreValue();

                        //10.获取反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 11.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 12.获取多行数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //处理数据
                        handle(tableName,eventType,rowDatasList);

                    }
                }
            }
        }

    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            // 获取订单表信息中新增的数据
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
        } else if ("order_detail".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            //获取订单表中新增和变化的数据
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            //获取用户表中新增和变化的数据
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }

    }

    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String kafkaTopicOrder) {
        //获取订单表中新增的数据
        for (CanalEntry.RowData rowData : rowDatasList) {
            //获取到当前行更新后的数据
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            //创建JSONObject对象用来封装解析出来的数据
            JSONObject jsonObject = new JSONObject();
            //获取每一列的数据
            for (CanalEntry.Column column : afterColumnsList) {
                //将一行数据封装到一个Json中
                jsonObject.put(column.getName(), column.getValue());
            }
            //打印测试
            System.out.println(jsonObject.toString());
//                System.out.println(jsonObject.toJSONString());测试，没啥区别
//            try {
//                Thread.sleep(new Random().nextInt(5000));
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            //将数据发送到Kafka中
            MyKafkaSender.send(kafkaTopicOrder, jsonObject.toString());
        }
    }
}
