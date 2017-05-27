package org.apache.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by huangweidong on 2017/5/25
 */
public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private int index = 0;

    private String[] sentences = {
            "This article is about the cat species that is commonly kept as a pet. For the cat family",
            "Since cats were venerated in ancient Egypt, they were commonly believed to have been domesticated there",
            "As of a 2007 study cats are the second most popular pet in the US by number of pets owned behind"
    };

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.sleep(1000);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
