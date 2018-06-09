package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import snippets.Snippets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mawu on 2018/5/26.
 */
public class SnipTest {
    private static PipelineOptions po = new PipelineOptionsFactory().fromArgs().create();
        private static Pipeline p = Pipeline.create(po);

        @Test
        public void testCoGroupByKeyTuple() throws IOException {
            // [START CoGroupByKeyTupleInputs]
            final List<KV<String, String>> emailsList = Arrays.asList(
                    KV.of("amy", "amy@example.com"),
                    KV.of("carl", "carl@example.com"),
                    KV.of("julia", "julia@example.com"),
                    KV.of("carl", "carl@email.com"));

            final List<KV<String, String>> phonesList = Arrays.asList(
                    KV.of("amy", "111-222-3333"),
                    KV.of("james", "222-333-4444"),
                    KV.of("amy", "333-444-5555"),
                    KV.of("carl", "444-555-6666"));

            PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
            PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));
            // [END CoGroupByKeyTupleInputs]

            // [START CoGroupByKeyTupleOutputs]
            final TupleTag<String> emailsTag = new TupleTag<>();
            final TupleTag<String> phonesTag = new TupleTag<>();

            // [END CoGroupByKeyTupleOutputs]

            PCollection<String> actualFormattedResults =
                    Snippets.coGroupByKeyTuple(emailsTag, phonesTag, emails, phones);

            p.run().waitUntilFinish();
        }
}
