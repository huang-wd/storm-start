package com.packtpub.storm.trident.message;


import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Date;

/**
 * Created by huangweidong on 2017/6/5
 */
public class NotifyMessageMapper implements MessageMapper {

    public String toMessageBody(TridentTuple tuple) {
        StringBuilder sb = new StringBuilder();
        sb.append("On " + tuple.getStringByField("timestamp") + " ");
        sb.append("the application \"" + tuple.getStringByField("logger") + "\" ");
        sb.append("changed alert state based on a threshold of " + tuple.getDoubleByField("threshold") + ".\n");
        sb.append("The last value was " + tuple.getDoubleByField("average") + "\n");
        sb.append("The last message was \"" + tuple.getStringByField("message") + "\"");
        return sb.toString();
    }

}
