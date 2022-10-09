package com.ef.demo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {


    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建JSON对象用于封装结果数据
        JSONObject result = new JSONObject();
        //获取库名 表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }
        Struct value = (Struct)sourceRecord.value();
        result.put("db",fields[1]);
        result.put("tableName",fields[2]);
        result.put("before",getData("before",value));
        result.put("after",getData("after",value));
        result.put("type",type);
        //输出数据
        collector.collect(result.toString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    public JSONObject getData(String pos, Struct value){
        Struct struct = value.getStruct(pos);
        JSONObject jsonObject = new JSONObject();
        if(struct != null){
            Schema schema = struct.schema();
            schema.fields().forEach( field -> {
                jsonObject.put(field.name(),struct.get(field));
            });
        }
        return jsonObject;
    }
}
