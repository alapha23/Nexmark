package handler;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryModel;
//import org.hbm.sdk.nexmark.queries.Query0;
import org.apache.beam.sdk.nexmark.queries.Query0Model;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

public class Handler implements RequestHandler<Map<String, String>, Context> {


    private static final Logger LOG = LoggerFactory.getLogger(Handler.class);

    @Override
    public Context handleRequest(Map<String, String> input, Context context) {
        LOG.info("MAYDAY");
        LOG.info(input.get("data"));
//        Gson gson = new Gson();
        byte[] bytesEncoded = input.get("data").getBytes(); //gson.fromJson(input, byte[].class);
        InputStream inStream = new ByteArrayInputStream(bytesEncoded);
        final Event e  = EventSerializer.deserialize(inStream);

        String [] arg = new String[0];
        NexmarkOptions options =
                PipelineOptionsFactory.fromArgs(arg).withValidation().as(NexmarkOptions.class);
        Iterable<NexmarkConfiguration> configurations = options.getSuite().getConfigurations(options);

        for (NexmarkConfiguration configuration : configurations) {
            LOG.info("Get query");
            // get query
            NexmarkQuery query = new Query0(configuration);//new Query0(configuration); //getNexmarkQuery();

            LOG.info("Create model");
            // create model
            NexmarkQueryModel model = new Query0Model(configuration); //getNexmarkQueryModel();

            LOG.info("Create pipeline");
            // create pipeline
            Pipeline p = Pipeline.create(options);
            NexmarkUtils.setupPipeline(configuration.coderStrategy, p);

            LOG.info("Generate events");
            // generate events
            PCollection<Event> source = p.apply("Query0" + ".ReadBounded", NexmarkUtils.batchEventsSource(configuration));
            //sourceEventsFromSynthetic(p);;//createSource(p, now);

            LOG.info("Apply query");
            // apply query
            PCollection<TimestampedValue<KnownSize>> results =
                    (PCollection<TimestampedValue<KnownSize>>) source.apply(query);

            LOG.info("Run pipeline");
            // run pipeline
            p.run();
        }
        return null;
//        Response responseBody = new Response("Hello, the current time is " + new Date());
//        Map<String, String> headers = new HashMap<>();
//        headers.put("X-Powered-By", "AWS Lambda & Serverless");
//        headers.put("Content-Type", "application/json");
/*        return ApiGatewayResponse.builder()
                .setStatusCode(200)
                .setObjectBody("Response")
                .setHeaders(headers)
                .build();
                */
    }


}