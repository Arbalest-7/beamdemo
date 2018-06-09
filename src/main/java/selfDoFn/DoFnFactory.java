package selfDoFn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Created by mawu on 2018/5/22.
 */

public class DoFnFactory {


    private static class WordCountLength extends DoFn<String, Integer> {
        @ProcessElement
        public void process(ProcessContext c) {
            String s = c.element();
            c.output(s.length());
            System.out.println(s);
        }
    }

    private static class DisplayElemet extends DoFn<Object, Object> {
        @ProcessElement
        public void process(ProcessContext c) {
            System.out.println(c.element());
            c.output(c);
        }
    }

    private static class Display extends DoFn<String, Integer> {
        @ProcessElement
        public void process(ProcessContext c) {
            String s = c.element();
            System.out.println(s);
        }
    }

    public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
        @Override
        public Integer apply(Iterable<Integer> input) {
            int sum = 0;
            for (int item : input) {
                sum += item;
            }
            return sum;
        }
    }

    public DoFn<String, Integer> getWordCountLength() {
        return new WordCountLength();
    }

    public DoFn<String, Integer> getDisplay() {
        return new Display();
    }

    public SerializableFunction<Iterable<Integer>, Integer> getSumInt() {
        return new SumInts();
    }

    public DoFn<Object, Object> getDisplayElemet() {
        return  new DisplayElemet();
    }

}
