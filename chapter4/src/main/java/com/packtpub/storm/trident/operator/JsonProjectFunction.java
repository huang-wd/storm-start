package com.packtpub.storm.trident.operator;

import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by huangweidong on 2017/6/5
 */
public class JsonProjectFunction extends BaseFunction {

    private Fields fields;

    public JsonProjectFunction(Fields fields) {
        this.fields = fields;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String json = tuple.getString(0);
        Map<String, Object> map = (Map<String, Object>) JSONValue.parse(json);
        Values values = new Values();
        if (map == null)
            return;

        for (String field : fields) {
            if (map.get(field) != null)
                values.add(map.get(field));
        }

        if (values.size() == fields.size()) {
            collector.emit(values);
        }
    }
}
