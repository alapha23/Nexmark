package handler;


import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.AuctionPrice;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query 2, 'Filtering. Find bids with specific auction ids and show their bid price. In CQL syntax:
 *
 * <pre>
 * SELECT Rstream(auction, price)
 * FROM Bid [NOW]
 * WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
 * </pre>
 *
 * <p>As written that query will only yield a few hundred results over event streams of arbitrary
 * size. To make it more interesting we instead choose bids for every {@code auctionSkip}'th
 * auction.
 */
public class Query2 extends NexmarkQuery {
    public Query2(NexmarkConfiguration configuration) {
        super(configuration, "Query2");
    }

    private PCollection<AuctionPrice> applyTyped(PCollection<Event> events) {
        return events
                // Only want the bid events.
                .apply(JUST_BIDS)

                // Select just the bids for the auctions we care about.
                .apply(Filter.by(bid -> bid.auction % configuration.auctionSkip == 0))

                // Project just auction id and price.
                .apply(
                        name + ".Project",
                        ParDo.of(
                                new DoFn<Bid, AuctionPrice>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        Bid bid = c.element();
                                        c.output(new AuctionPrice(bid.auction, bid.price));
                                    }
                                }));
    }

    @Override
    protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
        return NexmarkUtils.castToKnownSize(name, applyTyped(events));
    }
}
