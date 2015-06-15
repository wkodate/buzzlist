package com.wkodate.stormtwitter;

import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;
import org.atilika.kuromoji.Token;
import org.atilika.kuromoji.Tokenizer;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * TextSplitter.java.
 *
 * @author wkodate
 */
public class TextSplitter extends BaseFunction {

    /**
     * Tokenizer.
     */
    private Tokenizer tokenizer;

    @Override
    public final void prepare(
            final Map conf, final TridentOperationContext context) {
        this.tokenizer = Tokenizer.builder().build();
    }

    @Override
    public final void execute(
            final TridentTuple tuple, final TridentCollector collector) {
        String text = tuple.getStringByField("text");
        if (StringUtils.isEmpty(text)) {
            return;
        }
        List<Token> tokens = tokenizer.tokenize(text);
        for (Token token : tokens) {
            collector.emit(new Values(token.getSurfaceForm()));
        }
    }

}
