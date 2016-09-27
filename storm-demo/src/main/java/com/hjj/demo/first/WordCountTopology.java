package com.hjj.demo.first;

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
        SentenceSpout sentenceSpout = new SentenceSpout();
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("sentence-spout-id",sentenceSpout);

        //订阅sentence-spout-id，sentence-spout发射的tuple随机均匀分发给split-bolt
        topologyBuilder.setBolt("split-bolt-id",splitSentenceBolt).shuffleGrouping("sentence-spout-id");

        //按word分发到同一个bolt
        topologyBuilder.setBolt("count-bolt-id",wordCountBolt).fieldsGrouping("split-bolt-id",new Fields("word"));

        //流到唯一的bolt上
        topologyBuilder.setBolt("report-bolt-id",reportBolt).globalGrouping("count-bolt-id");

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
