package com.packtpub.storm.trident.operator;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Created by huangweidong on 2017/6/5
 */
public class BooleanFilter extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return tuple.getBoolean(0);
    }

}
