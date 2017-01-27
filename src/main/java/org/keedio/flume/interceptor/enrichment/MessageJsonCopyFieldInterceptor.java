package org.keedio.flume.interceptor.enrichment;

import com.eclipsesource.json.ParseException;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichmentInterceptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by danielsanchez on 26/1/17.
 */
public class MessageJsonCopyFieldInterceptor extends EnrichmentInterceptor {
    private Logger logger = Logger.getLogger(MessageJsonCopyFieldInterceptor.class);

    public MessageJsonCopyFieldInterceptor(Context context) {
        super(context);
    }

    @Override
    protected void addAdditionalFields(Event event, EnrichedEventBody enrichedBody) {
        String eventContext = context.getString(EnrichmentInterceptor.EVENT_TYPE);
        if (!eventContext.equals("enriched")) {
            return;  // Not in enrichment-json-schema
        }


        String message = enrichedBody.getMessage();

        String flattenMessage;
        try {
            flattenMessage = JsonFlattener.flatten(message);
        } catch (ParseException e) {
            logger.warn(e.getMessage());
            return;  // Empty message or not in JSON format
        }

        HashMap<String, String> messageMap;
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<HashMap<String,String>> typeRef = new TypeReference<HashMap<String,String>>() {};
        try {
            messageMap = objectMapper.readValue(flattenMessage, typeRef);
        } catch (IOException e) {
            //e.printStackTrace();
            logger.warn(e.getMessage());
            return;  // Message not in JSON format
        }

        Map<String, String> data = enrichedBody.getExtraData();
        data.putAll(messageMap);  // Dump all message items to extraData
    }

    public static class Builder implements Interceptor.Builder {
        private Context ctx;

        @Override
        public Interceptor build() {
            return new MessageJsonCopyFieldInterceptor(ctx);
        }

        @Override
        public void configure(Context context) {
            this.ctx = context;
        }
    }
}
