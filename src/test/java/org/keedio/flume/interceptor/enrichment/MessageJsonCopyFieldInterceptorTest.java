package org.keedio.flume.interceptor.enrichment;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.log4j.Logger;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichmentInterceptor;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichmentInterceptorAbstractTest;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by danielsanchez on 26/1/17.
 */
public class MessageJsonCopyFieldInterceptorTest extends EnrichmentInterceptorAbstractTest {
    private Logger logger = Logger.getLogger(MessageJsonCopyFieldInterceptorTest.class);

    @Test
    public void testNonEnrichedEmptyMessageInterception() {
        try {
            Event event = createEvent("");
            EnrichmentInterceptor interceptor = createMessageInterceptor("DEFAULT");
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getMessage().equals(""));

        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testNonEnrichedNotJsonMessageInterception() {
        try {
            Event event = createEvent("hello");
            EnrichmentInterceptor interceptor = createMessageInterceptor("DEFAULT");
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getMessage().equals("hello"));

        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }


    @Test
    public void testNonEnrichedSingleJsonMessageInterception() {
        try {
            Event event = createEvent("{\"some\": \"text\"}");
            EnrichmentInterceptor interceptor = createMessageInterceptor("DEFAULT");
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getMessage().equals("{\"some\": \"text\"}"));
            assertFalse(enrichedEventBody.getExtraData().containsKey("some"));

        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testEmptyMessageInterception() {
        try {
            Event event = createEvent("");
            EnrichmentInterceptor interceptor = createMessageInterceptor("enriched");
            Event intercepted = interceptor.intercept(event);

            // It should throw JsonParseException
            EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            junit.framework.Assert.fail();

        } catch (IOException e) {
            //e.printStackTrace();
        }
    }

    @Test
    public void testNotJsonMessageInterception() {
        try {
            Event event = createEvent("hello");
            EnrichmentInterceptor interceptor = createMessageInterceptor("enriched");
            Event intercepted = interceptor.intercept(event);

            // It should throw JsonParseException
            EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            junit.framework.Assert.fail();

        } catch (IOException e) {
            //e.printStackTrace();
        }
    }

    @Test
    public void testSingleJsonMessageInterception() {
        try {
            Event event = createEvent("{\"extraData\":{},\"message\":\"{\\\"flow_id\\\":\\\"something\\\"}\"}");
            EnrichmentInterceptor interceptor = createMessageInterceptor("enriched");
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getMessage().equals("{\"flow_id\":\"something\"}"));
            assertTrue(enrichedEventBody.getExtraData().containsKey("flow_id"));
            assertEquals(enrichedEventBody.getExtraData().get("flow_id"),"something");

        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testMultipleJsonMessageInterception() {
        try {
            Event event = createEvent("{\"extraData\":{},\"message\":\"{\\\"some\\\": \\\"text\\\", \\\"more\\\": \\\"fields\\\"}\"}");
            EnrichmentInterceptor interceptor = createMessageInterceptor("enriched");
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getMessage().equals("{\"some\": \"text\", \"more\": \"fields\"}"));
            assertTrue(enrichedEventBody.getExtraData().containsKey("some"));
            assertEquals(enrichedEventBody.getExtraData().get("some"),"text");
            assertTrue(enrichedEventBody.getExtraData().containsKey("more"));
            assertEquals(enrichedEventBody.getExtraData().get("more"),"fields");

        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testMultipleTypeJsonMessageInterception() {
        try {
            Event event = createEvent("{\"extraData\":{},\"message\":\"{\\\"string\\\": \\\"text\\\", \\\"boolean\\\": true, \\\"integer\\\": 123, \\\"float\\\": 123.45}\"}");
            EnrichmentInterceptor interceptor = createMessageInterceptor("enriched");
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getMessage().equals("{\"string\": \"text\", \"boolean\": true, \"integer\": 123, \"float\": 123.45}"));
            assertTrue(enrichedEventBody.getExtraData().containsKey("string"));
            assertEquals(enrichedEventBody.getExtraData().get("string"),"text");
            assertTrue(enrichedEventBody.getExtraData().containsKey("boolean"));
            assertEquals(enrichedEventBody.getExtraData().get("boolean"),"true");
            assertTrue(enrichedEventBody.getExtraData().containsKey("integer"));
            assertEquals(enrichedEventBody.getExtraData().get("integer"),"123");
            assertTrue(enrichedEventBody.getExtraData().containsKey("boolean"));
            assertEquals(enrichedEventBody.getExtraData().get("float"),"123.45");

        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testNestedJsonMessageInterception() {
        try {
            Event event = createEvent("{\"extraData\":{},\"message\":\"{\\\"string\\\": \\\"text\\\", \\\"nested\\\": {\\\"integer\\\": 123, \\\"float\\\": 123.45}}\"}");
            EnrichmentInterceptor interceptor = createMessageInterceptor("enriched");
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getMessage().equals("{\"string\": \"text\", \"nested\": {\"integer\": 123, \"float\": 123.45}}"));
            assertTrue(enrichedEventBody.getExtraData().containsKey("string"));
            assertEquals(enrichedEventBody.getExtraData().get("string"),"text");
            assertTrue(enrichedEventBody.getExtraData().containsKey("nested.integer"));
            assertEquals(enrichedEventBody.getExtraData().get("nested.integer"),"123");
            assertTrue(enrichedEventBody.getExtraData().containsKey("nested.float"));
            assertEquals(enrichedEventBody.getExtraData().get("nested.float"),"123.45");

        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testArrayJsonMessageInterception() {
        try {
            Event event = createEvent("{\"extraData\":{},\"message\":\"{\\\"string\\\": \\\"text\\\", \\\"array\\\": [123, 123.45]}\"}");
            EnrichmentInterceptor interceptor = createMessageInterceptor("enriched");
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getMessage().equals("{\"string\": \"text\", \"array\": [123, 123.45]}"));
            assertTrue(enrichedEventBody.getExtraData().containsKey("string"));
            assertEquals(enrichedEventBody.getExtraData().get("string"),"text");
            assertTrue(enrichedEventBody.getExtraData().containsKey("array[0]"));
            assertEquals(enrichedEventBody.getExtraData().get("array[0]"),"123");
            assertTrue(enrichedEventBody.getExtraData().containsKey("array[1]"));
            assertEquals(enrichedEventBody.getExtraData().get("array[1]"),"123.45");

        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }


    protected MessageJsonCopyFieldInterceptor createMessageInterceptor(String eventType) {
        Context context = new Context();
        context.put(EnrichmentInterceptor.EVENT_TYPE, eventType);

        MessageJsonCopyFieldInterceptor.Builder builder = new MessageJsonCopyFieldInterceptor.Builder();
        builder.configure(context);
        MessageJsonCopyFieldInterceptor interceptor = (MessageJsonCopyFieldInterceptor) builder.build();
        interceptor.initialize();
        return interceptor;
    }
}
