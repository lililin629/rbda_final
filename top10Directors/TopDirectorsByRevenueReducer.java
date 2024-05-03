import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;

public class TopDirectorsByRevenueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private TreeMap<Long, String> topDirectors = new TreeMap<>();

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }

        topDirectors.put(sum, key.toString());
        if (topDirectors.size() > 10) {
            topDirectors.remove(topDirectors.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : topDirectors.descendingMap().entrySet()) {
            context.write(new Text(entry.getValue()), new LongWritable(entry.getKey()));
        }
    }
}
