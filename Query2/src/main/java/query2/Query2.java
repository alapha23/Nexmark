package query2;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.AuctionPrice;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

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
public class Query2 extends NexmarkQueryTransform<AuctionPrice> {
    private final int auctionSkip;

    public Query2(NexmarkConfiguration configuration) {
        super("Query2");
        this.auctionSkip = configuration.auctionSkip;
    }

    @Override
    public PCollection<AuctionPrice> expand(PCollection<Event> events) {
        return events
                // Only want the bid events.
                .apply(NexmarkQueryUtil.JUST_BIDS)

                // Select just the bids for the auctions we care about.
                .apply(Filter.by(bid -> bid.auction % this.auctionSkip == 0))

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
}
