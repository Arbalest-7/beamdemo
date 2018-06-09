package snippets;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by mawu on 2018/5/25.
 */
public class Snippets {
    /** Helper function to format results in coGroupByKeyTuple. */
    public static String formatCoGbkResults(
            String name, Iterable<String> emails, Iterable<String> phones) {

        List<String> emailsList = new ArrayList<>();
        for (String elem : emails) {
            emailsList.add("'" + elem + "'");
        }
        Collections.sort(emailsList);
        String emailsStr = "[" + String.join(", ", emailsList) + "]";

        List<String> phonesList = new ArrayList<>();
        for (String elem : phones) {
            phonesList.add("'" + elem + "'");
        }
        Collections.sort(phonesList);
        String phonesStr = "[" + String.join(", ", phonesList) + "]";

        return name + "; " + emailsStr + "; " + phonesStr;
    }

    /** Using a CoGroupByKey transform. */
    public static PCollection<String> coGroupByKeyTuple(
            TupleTag<String> emailsTag,
            TupleTag<String> phonesTag,
            PCollection<KV<String, String>> emails,
            PCollection<KV<String, String>> phones) {

        // [START CoGroupByKeyTuple]99
        PCollection<KV<String, CoGbkResult>> results =
                KeyedPCollectionTuple
                        .of(emailsTag, emails)
                        .and(phonesTag, phones)
                        .apply(CoGroupByKey.create());

        PCollection<String> contactLines = results.apply(ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        String name = e.getKey();
                        Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
                        Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
                        String formattedResult = Snippets.formatCoGbkResults(name, emailsIter, phonesIter);
                        System.out.println(formattedResult);
                        c.output(formattedResult);
                    }
                }
        ));
        // [END CoGroupByKeyTuple]
        return contactLines;
    }
}
