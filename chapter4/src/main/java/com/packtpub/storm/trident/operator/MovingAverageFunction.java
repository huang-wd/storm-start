package com.packtpub.storm.trident.operator;

import com.packtpub.storm.trident.model.EWMA;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by huangweidong on 2017/6/5
 */
public class MovingAverageFunction extends BaseFunction {

    private static final Logger LOG = LoggerFactory.getLogger(MovingAverageFunction.class);

    private EWMA ewma;
    private EWMA.Time emitRatePer;

    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public MovingAverageFunction(EWMA ewma, EWMA.Time emitRatePer) {
        this.ewma = ewma;
        this.emitRatePer = emitRatePer;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String dateStr = tuple.getString(0);
        Date date = null;
        try {
            date = formatter.parse(dateStr);
        } catch (ParseException e) {
            LOG.debug("时间解析失败，{}", dateStr);
        }

        if (date != null) {
            this.ewma.mark(date.getTime());
            collector.emit(new Values(this.ewma.getAverageRatePer(this.emitRatePer)));
        }
    }
}
