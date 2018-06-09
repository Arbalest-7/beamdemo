package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.*;
import org.junit.Test;
import selfDoFn.DoFnFactory;

import java.util.Arrays;
import java.util.List;


/**
 * Created by mawu on 2018/5/18.
 */
public class Demo {
    private static PipelineOptions po = new PipelineOptionsFactory().fromArgs().create();
    private static Pipeline p = Pipeline.create(po);
    private static String filePath = "input.property";


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

    final TupleTag<String> emailsTag = new TupleTag<>();
    final TupleTag<String> phonesTag = new TupleTag<>();

    PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
    PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));


    final List<KV<String, CoGbkResult>> expectedResults = Arrays.asList(
            KV.of("amy", CoGbkResult
                    .of(emailsTag, Arrays.asList("amy@example.com"))
                    .and(phonesTag, Arrays.asList("111-222-3333", "333-444-5555"))),
            KV.of("carl", CoGbkResult
                    .of(emailsTag, Arrays.asList("carl@email.com", "carl@example.com"))
                    .and(phonesTag, Arrays.asList("444-555-6666"))),
            KV.of("james", CoGbkResult
                    .of(emailsTag, Arrays.asList())
                    .and(phonesTag, Arrays.asList("222-333-4444"))),
            KV.of("julia", CoGbkResult
                    .of(emailsTag, Arrays.asList("julia@example.com"))
                    .and(phonesTag, Arrays.asList())));

    private static List<String> LINES = Arrays.asList(
            "eat better, live better",
            "hello jerry",
            "hello tom",
            "fine thankyou"
    );

    @Test
    public void countWord() throws Exception{
        DoFnFactory doFnFactory = new DoFnFactory();

        p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply(ParDo.of(doFnFactory.getDisplayElemet()));
//        PCollection<String> p1 = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
//              .apply(ParDo.of(doFnFactory.getWordCountLength()));

//
//        PCollection<Integer> wordLengths = p1.apply("ComputeWordLength",
//              ParDo.of(new DoFn<String, Integer>() {
//          @ProcessElement
//            public void process(ProcessContext c) {
//              c.output(c.element().length());
//            }
//        }));
//
//        PCollection<Integer> wordLengths2 = p1.apply(MapElements.into(TypeDescriptors.integers()).via((String word) -> word.length()));



      p.run().waitUntilFinish();

//              .apply(ParDo.of(doFnFactory.getDisplay()));


    }

    public static void main(String[] args) {
        Demo demo = new Demo();
        try {
            demo.countWord();
        }catch (Exception e) {
            //todo
        }
    }
}
