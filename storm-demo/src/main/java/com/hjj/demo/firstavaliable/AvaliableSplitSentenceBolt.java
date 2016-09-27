package com.hjj.demo.firstavaliable;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by jianjun.hao
 * 2016/9/20.
 */
public class AvaliableSplitSentenceBolt extends BaseRichBolt {

    private OutputCollector outputCollector;


    //storm配置
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    //处理逻辑
    @Override
    public void execute(Tuple tuple) {

        String sentence = tuple.getStringByField("sentence");

        String[] words = sentence.split(" ");

        for(String word:words){
            outputCollector.emit(tuple,new Values(word));
        }
//        自动调用确认
//        outputCollector.ack(tuple);


    }

    //设置流传递对象
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
