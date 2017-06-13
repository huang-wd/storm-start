package com.packtpub.storm.trident.topology;

import com.packtpub.storm.trident.message.NotifyMessageMapper;
import com.packtpub.storm.trident.model.EWMA;
import com.packtpub.storm.trident.operator.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

/**
 * Created by huangweidong on 2017/6/5
 */
public class LogAnalysisTopology {
    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();

        ZkHosts zkHosts = new ZkHosts("zk.storm.com");

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zkHosts, "foo");

        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        Stream spoutStream = topology.newStream("kafka-stream", spout);

        Fields jsonFields = new Fields("level", "timestamp", "message", "logger");

        Stream parsedStream = spoutStream.each(new Fields("str"), new JsonProjectFunction(jsonFields), jsonFields);

        //drop the unparsed JSON to reduce tuple size
        parsedStream = parsedStream.project(jsonFields);
        EWMA ewma = new EWMA().sliding(1.0, EWMA.Time.MINUTES).withAlpha(EWMA.ONE_MINUTE_ALPHA);
        Stream averageStream = parsedStream.each(new Fields("timestamp"), new MovingAverageFunction(ewma, EWMA.Time.MINUTES), new Fields("average"));
        ThresholdFilterFunction tff = new ThresholdFilterFunction(50D);
        Stream thresholdStream = averageStream.each(new Fields("average"), tff, new Fields("change", "threshold"));
        Stream filteredStream = thresholdStream.each(new Fields("change"), new BooleanFilter());
        filteredStream.each(filteredStream.getOutputFields(), new XMPPFunction(new NotifyMessageMapper()), new Fields());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();

        conf.put(XMPPFunction.XMPP_USER, "storm@xmpp.storm.com");
        conf.put(XMPPFunction.XMPP_PASSWORD, "root");
        conf.put(XMPPFunction.XMPP_SERVER, "xmpp.storm.com");
        conf.put(XMPPFunction.XMPP_TO, "admin@xmpp.storm.com");

        conf.setMaxSpoutPending(5);

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("log-analysis", conf, buildTopology());
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology());
        }
    }

}
