package com.packtpub.storm.trident.message;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.packtpub.storm.trident.formatter.Formatter;

/**
 * Created by huangweidong on 2017/6/5
 */
public class MessageFormatter implements Formatter {
    public String format(ILoggingEvent event) {
        return event.getFormattedMessage();
    }
}
