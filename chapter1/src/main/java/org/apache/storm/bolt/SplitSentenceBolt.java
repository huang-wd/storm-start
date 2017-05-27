package org.apache.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by huangweidong on 2017/5/25
 */
public class SplitSentenceBolt extends BaseRichBolt {
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    public void execute(Tuple input) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
