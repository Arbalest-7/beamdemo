package combine;

import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Created by mawu on 2018/5/26.
 */
public class CombineDemo {


    public MeanInts getMeanInts() {
        return new MeanInts();
    }

    public SumFn getSumFn() {
        return new SumFn();
    }

    /**
     * Example AccumulatingCombineFn.
     */
    protected static class MeanInts extends
            org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn<Integer, MeanInts.CountSum, Double> {
        private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
        private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

        static class CountSum implements
                org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator<Integer, CountSum, Double> {
            long count = 0;
            double sum = 0.0;

            CountSum(long count, double sum) {
                this.count = count;
                this.sum = sum;
            }

            @Override
            public void addInput(Integer element) {
                count++;
                sum += element.doubleValue();
            }

            @Override
            public void mergeAccumulator(CountSum accumulator) {
                count += accumulator.count;
                sum += accumulator.sum;
            }

            @Override
            public Double extractOutput() {
                return count == 0 ? 0.0 : sum / count;
            }

            @Override
            public int hashCode() {
                return Objects.hash(count, sum);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (!(obj instanceof CountSum)) {
                    return false;
                }
                CountSum other = (CountSum) obj;
                return this.count == other.count
                        && (Math.abs(this.sum - other.sum) < 0.1);
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                        .add("count", count)
                        .add("sum", sum)
                        .toString();
            }
        }


        /**
         * A {@link Coder} for {@link MeanInts.CountSum}.
         */
        private static class CountSumCoder extends AtomicCoder<MeanInts.CountSum> {

            private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
            private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();
            @Override
            public void encode(MeanInts.CountSum value, OutputStream outStream) throws IOException {
                LONG_CODER.encode(value.count, outStream);
                DOUBLE_CODER.encode(value.sum, outStream);
            }

            @Override
            public MeanInts.CountSum decode(InputStream inStream) throws IOException {
                long count = LONG_CODER.decode(inStream);
                double sum = DOUBLE_CODER.decode(inStream);
                return new MeanInts.CountSum(count, sum);
            }

            @Override
            public void verifyDeterministic() throws NonDeterministicException {
            }

            @Override
            public boolean isRegisterByteSizeObserverCheap(
                    MeanInts.CountSum value) {
                return true;
            }

            @Override
            public void registerByteSizeObserver(
                    MeanInts.CountSum value, ElementByteSizeObserver observer)
                    throws Exception {
                LONG_CODER.registerByteSizeObserver(value.count, observer);
                DOUBLE_CODER.registerByteSizeObserver(value.sum, observer);
            }
        }

        @Override
        public CountSum createAccumulator() {
            return new CountSum(0, 0.0);
        }



        @Override
        public Coder<CountSum> getAccumulatorCoder(
                CoderRegistry registry, Coder<Integer> inputCoder) {
            return new CountSumCoder();
        }
    }


    public static class SumFn extends
            org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn<Integer, SumFn.Accum, Double> {

        public static class Accum implements
                org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator<Integer, SumFn.Accum, Double> {

            int sum = 0;
            int count = 0;


            public Accum(int sum, int count) {
                this.sum = sum;
                this.count = count;
            }

            @Override
            public void mergeAccumulator(Accum accum) {
                count += accum.count;
                sum += accum.sum;
            }

            @Override
            public void addInput(Integer integer) {
                count++;
                sum += integer;
            }

            @Override
            public Double extractOutput() {
                return Double.valueOf(sum);
            }

        }



        /**
         * A {@link Coder} for {@link MeanInts.CountSum}.
         */
        private static class SumFnCoder extends AtomicCoder<SumFn.Accum> {

            private static final Coder<Integer> LONG_CODER = BigEndianIntegerCoder.of();
            private static final Coder<Integer> DOUBLE_CODER = BigEndianIntegerCoder.of();


            @Override
            public void encode(SumFn.Accum value, OutputStream outStream) throws IOException {
                LONG_CODER.encode(value.count, outStream);
                DOUBLE_CODER.encode(value.sum, outStream);
            }

            @Override
            public SumFn.Accum decode(InputStream inStream) throws IOException {
                int count = LONG_CODER.decode(inStream);
                int sum = DOUBLE_CODER.decode(inStream);
                return new SumFn.Accum(sum, count);
            }

            @Override
            public void verifyDeterministic() throws NonDeterministicException {
            }

            @Override
            public boolean isRegisterByteSizeObserverCheap(
                    SumFn.Accum value) {
                return true;
            }

            @Override
            public void registerByteSizeObserver(
                    SumFn.Accum value, ElementByteSizeObserver observer)
                    throws Exception {
                LONG_CODER.registerByteSizeObserver(value.count, observer);
                DOUBLE_CODER.registerByteSizeObserver(value.sum, observer);
            }
        }

        @Override
        public SumFn.Accum createAccumulator() {
            return new SumFn.Accum(0,0);
        }

        @Override
        public Coder<Accum> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputCoder) throws CannotProvideCoderException {
            return new SumFnCoder();
        }
    }




}
