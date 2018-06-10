package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import selfDoFn.DoFnFactory;


import java.util.Arrays;

/**
 * Created by mawu on 2018/6/9.
 */
public class WordCount {

    private static PipelineOptions po = new PipelineOptionsFactory().fromArgs().create();
    private static Pipeline p = Pipeline.create(po);
    private static String filePath = "src/main/resources/input.property";
    private static DoFnFactory doFnFactory = new DoFnFactory();



    @Test
    public void wordCount() {
        //read
        p.apply(TextIO.read().from(filePath))
                //extract
                .apply("ExtractWords", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String word) -> Arrays.asList(word.split(" "))))
                //filter
                .apply(Filter.by((String word) -> !word.isEmpty()))
                //count
                .apply(Count.perElement())
                //form
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + " : " + wordCount.getValue()))
                //write
                .apply(TextIO.write().to("wordcounts"));
        p.run().waitUntilFinish();

    }
}
