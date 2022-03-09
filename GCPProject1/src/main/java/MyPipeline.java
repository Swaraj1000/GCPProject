import com.google.api.Context;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Mutation;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class Main {
        private static final Logger LOG = LoggerFactory.getLogger(Main.class);

        private static TupleTag<AccountTable> ValidMessage = new TupleTag<AccountTable>(){};
        private static TupleTag<String> DlqMessage = new TupleTag<String>(){};

        public interface Options extends DataflowPipelineOptions{
            @Description("Bigquery Table Name")
            String getTableName();
            void setTableName(String tableName);

            @Description("Pubsub Subscription")
            String getpubsubTopic();
            void setpubsubTopic(String pubsubTopic);

            @Description("DLQ Topic")
            String getdlqTopic();
            void setdlqTopic(String dlqTopic);
            }
        public static void main(String[] args) {
                PipelineOptionsFactory.register(Options.class);
                Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
                run(options);
        }
        static class JSONtoAccountTable extends DoFn<String,AccountTable>{
                @ProcessElement
            public void processElement(@Element String json,ProcessContext processContext)throws Exception{
                    Gson gson = new Gson();
                    try {
                        AccountTable account = gson.fromJson(json,AccountTable.class);
                        processContext.output(ValidMessage,account);
                    } catch (Exception exception){
                        processContext.output(DlqMessage,json);
                    }
                }
        }

        public static final Schema rawSchema = Schema
                .builder()
                .addInt32Field("id")
                .addStringField("name")
                .addStringField("surname")
                .build();

        public static PipelineResult run(Options options){
            Pipeline pipeline = Pipeline.create(options);
            PCollectionTuple pubsubMessage = pipeline.apply("Read Message",PubsubIO.readStrings().fromSubscription(options.getpubsubTopic()))
                    .apply("Validate",ParDo.of(new JSONtoAccountTable()).withOutputTags(ValidMessage, TupleTagList.of(DlqMessage)));

            PCollection<AccountTable> ValidData = pubsubMessage.get(ValidMessage);
            PCollection<String> InvalidMessage = pubsubMessage.get(DlqMessage);
            ValidData.apply("Convert",ParDo.of(new DoFn<AccountTable, String>() {
                @ProcessElement
                public void convert(ProcessContext context){
                    Gson g = new Gson();
                    String gsonString = g.toJson(context.element());
                    context.output(gsonString);

                }
            })).apply("Json To row",JsonToRow.withSchema(rawSchema)).
                    apply("write message to bigquery table",BigQueryIO.<Row>write().to(options.getTableName())
                            .useBeamSchema()
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

            InvalidMessage.apply("Message to DLQ",PubsubIO.writeStrings().to(options.getdlqTopic()));
            return pipeline.run();
        }

}
