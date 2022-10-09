package com.ef.demo;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.SeekBinlogToTimestampFilter;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCMysqlTest {

    public static void main(String[] args) throws Exception {

        //1.获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CK 5秒钟开启一次checkpoint，生产环境一般是分钟级别的
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //精准一次的模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/ck"));

        //2.通过flinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("192.168.0.104")
                .port(3306)
                .username("root")
                .password("123456")
                //所要监听的数据库
                .databaseList("cdc_test")
                .tableList("cdc_test.user_info")
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new CustomerDeserializationSchema())
                //先用查询的方式读取过来，再切换到binlog最新的位置开始读取后来新增的数据
                .startupOptions(StartupOptions.initial())
                //earliest 从表开始创建的binlog位置读取，binlog中需要记录建库建表操作，负责会失败
                //latest 只读取开始这个程序以后所发生的变化
                //其他可以指定位置或者时间戳开始读取
                .build();

        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);

        //3.数据打印
        stringDataStreamSource.print();

        //4.启动任务
        env.execute("FlinkCDC");

    }
}
