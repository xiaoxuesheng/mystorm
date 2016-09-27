package com.hjj.demo.first;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by jianjun.hao
 * 2016/9/20.
 */
public class ReportBolt extends BaseRichBolt {

    private HashMap<String,Long> counts = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new  HashMap<String,Long>();
    }

    @Override
    public void execute(Tuple tuple) {


        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word,count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup(){


        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(new File("D:\\11.txt"));
            fileOutputStream.write(counts.toString().getBytes());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


//        System.out.println("-------------");
//        List<String> keys = new ArrayList<String>();
//        keys.addAll(this.counts.keySet());
//
//        Collections.sort(keys);
//        for(String key:keys){
//            System.out.println(key+":"+this.counts.get(key));
//        }

    }
}
