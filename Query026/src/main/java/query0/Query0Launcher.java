package query0;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.nexmark.*;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryModel;
import org.apache.beam.sdk.nexmark.queries.Query0Model;
import org.apache.beam.sdk.nexmark.sources.UnboundedEventSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;

public class Query0Launcher {

    private static final Logger LOG = LoggerFactory.getLogger(Query0Launcher.class.getName());

    public static void main(String[] args) throws IOException {
        NexmarkOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(NexmarkOptions.class);
        Iterable<NexmarkConfiguration> configurations = options.getSuite().getConfigurations(options);

        for (NexmarkConfiguration configuration : configurations) {
            LOG.info("Get query");
            // get query
            NexmarkQuery query =  new Query0(configuration);//new Query0(configuration); //getNexmarkQuery();

            /*
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
            */

            final UnboundedEventSource usrc = new UnboundedEventSource(
                    NexmarkUtils.standardGeneratorConfig(configuration),
                    configuration.numEventGenerators,
                    configuration.watermarkHoldbackSec, configuration.isRateLimited);
            LOG.info("Configuration: {}", configuration);
            LOG.info("# of event generator: {}", configuration.numEventGenerators);
            final List<UnboundedEventSource> sources = usrc.split(configuration.numEventGenerators, options);
            final UnboundedSource.UnboundedReader<Event> reader = sources.get(0).createReader(null, null);
            reader.start();
            final Event e = reader.getCurrent();
            //reader.advance();

            ((Query0) query).runQuery(e);
/*            LOG.info("Apply query");
            // apply query
            PCollection<TimestampedValue<KnownSize>> results =
                    (PCollection<TimestampedValue<KnownSize>>) source.apply(query);
            LOG.info("Run pipeline");
            // run pipeline
            p.run();
            */
        }
    }

}
