package demo;

import combine.CombineDemo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import selfDoFn.DoFnFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by mawu on 2018/5/26.
 */
public class DemoTest {

    private static PipelineOptions po = new PipelineOptionsFactory().fromArgs().create();
    private static Pipeline p = Pipeline.create(po);
    private static DoFnFactory factory = new DoFnFactory();

    @Test
    public void testSum() {
        List<Integer> nums = Arrays.asList(1,2,3,4,5);
        PCollection<Integer> integerPCollection = p.apply(Create.of(nums));
        integerPCollection.apply(Combine.globally(factory.getSumInt()).withoutDefaults())
        .apply(ParDo.of(factory.getDisplayElemet()));
        p.run().waitUntilFinish();

    }


    @Test
    public void groupBySumTest() {
        final List<KV<String, Integer>> wordCount = Arrays.asList(
                KV.of("cat",1),
                KV.of("cat",2),
                KV.of("cat",3),
                KV.of("ss",4),
                KV.of("ss",5),
                KV.of("simon",1),
                KV.of("simon",10)

                );
        PCollection<KV<String, Integer>> input = p.apply(Create.of(wordCount));

        PCollection<KV<String, Double>> avgAccuracyPerPlayer =
                input.apply(Combine.<String, Integer, Double>perKey(
                        new CombineDemo().getSumFn()));

        avgAccuracyPerPlayer.apply(ParDo.of(factory.getDisplayElemet()));
        p.run().waitUntilFinish();

    }
}
