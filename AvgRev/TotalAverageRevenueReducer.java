import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TotalAverageRevenueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        int count = 0;
        for (LongWritable val : values) {
            sum += val.get();
            count++;
        }
        if (count > 0) {
            long average = sum / count;
            context.write(new Text("Average Revenue"), new LongWritable(average));
        }
    }
}
