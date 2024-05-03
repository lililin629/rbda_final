import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

public class TopYearsByRevenueReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
    private TreeMap<Long, Integer> topYears = new TreeMap<>();

    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }

        topYears.put(sum, key.get());
        if (topYears.size() > 10) {
            topYears.remove(topYears.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Entry<Long, Integer> entry : topYears.descendingMap().entrySet()) {
            context.write(new IntWritable(entry.getValue()), new LongWritable(entry.getKey()));
        }
    }
}
