package com.packtpub.storm.trident.message;


import org.apache.storm.trident.tuple.TridentTuple;

import java.io.Serializable;

/**
 * Created by huangweidong on 2017/6/5
 */
public interface MessageMapper extends Serializable {
    String toMessageBody(TridentTuple tuple);
}
