package invoke;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;

import com.google.gson.Gson;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.UnboundedEventSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.List;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class.getName());
    public static void main(String args[]){
        final NexmarkOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(NexmarkOptions.class);

        Iterable<NexmarkConfiguration> configurations = options.getSuite().getConfigurations(options);

        for (NexmarkConfiguration configuration : configurations) {
            final UnboundedEventSource usrc = new UnboundedEventSource(
                    NexmarkUtils.standardGeneratorConfig(configuration),
                    configuration.numEventGenerators,
                    configuration.watermarkHoldbackSec, configuration.isRateLimited);
            LOG.info("Configuration: {}", configuration);
            LOG.info("# of event generator: {}", configuration.numEventGenerators);

            final List<UnboundedEventSource> sources = usrc.split(configuration.numEventGenerators, options);
            final UnboundedSource.UnboundedReader<Event> reader = sources.get(0).createReader(null, null);
            try {
                if (reader.start()) {
                    final Event obj = reader.getCurrent();
                    System.out.println("Event: "+obj.toString());

                    // send the first split of source to lambda

                    // 1 Define the AWS Region in which the function is to be invoked
                    Regions region = Regions.fromName("ap-northeast-1");

                    // 2 Instantiate AWSLambdaClientBuilder to build the Lambda client
                    AWSLambdaClientBuilder builder = AWSLambdaClientBuilder.standard()
                            .withRegion(region);

                    // 3 Build the client, which will ultimately invoke the function
                    AWSLambda client = builder.build();

                    // 4 Create an InvokeRequest with required parameters
                    //byte [] bytesEncoded = Base64.getEncoder().encode(obj.toString().getBytes());
                    byte [] bytesEncoded = EventSerializer.serialize(obj);
//                    Gson gson = new Gson();
//                    String json = gson.toJson(bytesEncoded, byte[].class);
                    String json = "{ \"data\": \"" +bytesEncoded.toString()+"\" }";
                    System.out.println("Passing json: "+json);

                    InvokeRequest req = new InvokeRequest()
                            .withFunctionName("gao-dev-nexmark-query0")
                            .withPayload(json); // optional

                    // 5 Invoke the function and capture response
                    InvokeResult result = client.invoke(req);
                    System.out.println("Result "+result.toString());
                }
            } catch(Exception e){LOG.info("Bang");}
        }
    }
}