package com.hjj.demo.firstavaliable;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;

/**
 * Created by jianjun.hao
 * 2016/9/10.
 */
public class AvaliableSentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };

    private int index = 0;



    //storm组件发射的数据流
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("sentence"));

    }

    //spout配置
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    //发射tuple
    @Override
    public void nextTuple() {
        UUID uuid = UUID.randomUUID();

        this.collector.emit(new Values(sentences[index]),uuid);
        index++;

        if (index >= sentences.length) {
            index = 0;
        }

    }

    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
    }

    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }
}
