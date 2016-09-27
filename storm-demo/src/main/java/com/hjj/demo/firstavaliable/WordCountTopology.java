package com.hjj.demo.firstavaliable;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


/**
 * Created by jianjun.hao
 * 2016/9/20.
 */
public class WordCountTopology {
    public static void main(String[] args){
        AvaliableSentenceSpout avaliableSentenceSpout = new AvaliableSentenceSpout();
        AvaliableSplitSentenceBolt splitSentenceBolt = new AvaliableSplitSentenceBolt();
        AvaliableWordCountBolt avaliableWordCountBolt = new AvaliableWordCountBolt();
        AvaliableReportBolt avaliableReportBolt = new AvaliableReportBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("sentence-spout-id", avaliableSentenceSpout);

        //订阅sentence-spout-id，sentence-spout发射的tuple随机均匀分发给split-bolt
        topologyBuilder.setBolt("split-bolt-id",splitSentenceBolt).shuffleGrouping("sentence-spout-id");

        //按word分发到同一个bolt
        topologyBuilder.setBolt("count-bolt-id", avaliableWordCountBolt).fieldsGrouping("split-bolt-id",new Fields("word"));

        //流到唯一的bolt上
        topologyBuilder.setBolt("report-bolt-id", avaliableReportBolt).globalGrouping("count-bolt-id");

        Config config = new Config();
        config.setDebug(true);
//        config.setMessageTimeoutSecs(15);
//        config.setMaxSpoutPending(5000);
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("word-count-topology",config,topologyBuilder.createTopology());


        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        localCluster.killTopology("word-count-topology");
        localCluster.shutdown();

    }
}
