package com.packtpub.storm.trident.operator;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangweidong on 2017/6/5
 */
public class ThresholdFilterFunction extends BaseFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ThresholdFilterFunction.class);

    private State last = State.BELOW;
    private double threshold;

    public ThresholdFilterFunction(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        double val = tuple.getDouble(0);
        State newState = val < this.threshold ? State.BELOW : State.ABOVE;
        boolean stateChange = this.last != newState;
        collector.emit(new Values(stateChange, threshold));
        this.last = newState;
        LOG.debug("State change? --> {}", stateChange);
    }

    private static enum State {
        BELOW, ABOVE
    }

}
