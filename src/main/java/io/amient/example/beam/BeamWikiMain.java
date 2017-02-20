package io.amient.example.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class BeamWikiMain {
    private static final Logger log = LoggerFactory.getLogger(BeamWikiMain.class);

    public static void main(String[] args) throws Exception {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline pipeline = Pipeline.create(options);

//        PCollection<String> edits = pipeline.apply(Create.of("Hello", "world"));

        PCollection<String> edits = pipeline.apply(Read.from(
                BeamIRCSource.of("irc.wikimedia.org", 6667, "#en.wikipedia,#en.wiktionary,#en.wikinews")));

        //TODO aggregate number of edits by username in 5 second window

        edits.apply(ParDo.of(new PrintFn<>()));

        pipeline.run().waitUntilFinish();

    }


    public static class BeamIRCSource extends UnboundedSource<String, NoOpCheckpointMark> {

        public static BeamIRCSource of(String host, int port, String channels) {
            return null;
        }

        public BeamIRCSource(String host, int port, String[] channels) {
        }

        @Override
        public List<? extends UnboundedSource<String, NoOpCheckpointMark>> generateInitialSplits(int desiredNumSplits, PipelineOptions options) throws Exception {
            System.out.println("Desired num split = " + desiredNumSplits);
            return null;
        }

        @Override
        public UnboundedReader<String> createReader(PipelineOptions options, @Nullable NoOpCheckpointMark checkpointMark) throws IOException {
            return null;
        }

        @Nullable
        @Override
        public Coder<NoOpCheckpointMark> getCheckpointMarkCoder() {
            return null;
        }

        @Override
        public void validate() {}

        @Override
        public Coder<String> getDefaultOutputCoder() {
            return SerializableCoder.of(String.class);
        }

    }

    private static class NoOpCheckpointMark implements UnboundedSource.CheckpointMark {
        @Override
        public void finalizeCheckpoint() throws IOException {}
    }

    private static class PrintFn<T> extends DoFn<T, T> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element().toString());
        }
    }
}
