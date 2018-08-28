package io.amient.example.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

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

        private final String host;
        private final int port;
        private final String[] channels;

        public static BeamIRCSource of(String host, int port, String channels) {
            return new BeamIRCSource(host, port, channels.split(","));
        }

        public BeamIRCSource(String host, int port, String[] channels) {
//            client = new IRCSource(host, port, channels)
            this.host = host;
            this.port = port;
            this.channels = channels;
            log.info(host + ":" + port + " " + Arrays.toString(channels));
        }

        @Override
        public List<BeamIRCSource> generateInitialSplits(int desiredNumSplits, PipelineOptions options) throws Exception {
            List<BeamIRCSource> sources = new ArrayList();
            int channelsPerSplit = Math.max(1, channels.length / desiredNumSplits);
            for(int s = 0; s < Math.min(channels.length, desiredNumSplits); s ++) {
                int splitSize = Math.min(channelsPerSplit, channels.length - s * channelsPerSplit);
                String[] split = new String[splitSize];
                for (int c = 0 ; c < splitSize; c ++ ) {
                    split[c] = channels[s * channelsPerSplit + c];
                }
                log.info("Split " + s + " " + Arrays.toString(split));
                sources.add(new BeamIRCSource(host, port, split));
            }
            return sources;
        }

        @Override
        public UnboundedReader<String> createReader(PipelineOptions options, @Nullable NoOpCheckpointMark checkpointMark) throws IOException {
            //TODO UnboundedReader that wraps around the utility class IRCSource
            return null;
        }

        @Nullable
        @Override
        public Coder<NoOpCheckpointMark> getCheckpointMarkCoder() {
            //TODO is there a NoOpCheckpointMarkCoder
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
