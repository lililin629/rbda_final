import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.Scanner;

public class CSVMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Use Scanner to read the line using a comma delimiter while respecting quotes
        Scanner scanner = new Scanner(value.toString());
        scanner.useDelimiter(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        try {
            if (scanner.hasNext()) {
                String movieName = scanner.next().trim();
                String movieDate = scanner.next().trim();
                scanner.next(); // Skip 'Serie Name'
                scanner.next(); // Skip 'Serie Date'
                String movieType = scanner.next().trim();
                scanner.next(); // Skip 'Number of Votes'
                String movieRevenue = scanner.next().trim();
                String score = scanner.next().trim();
                String metascore = scanner.next().trim();
                scanner.next(); // Skip 'Time Duration'
                String director = scanner.next().trim();

                // Format the output as needed
                String output = movieName + "," + movieDate + "," + movieType + "," + movieRevenue + "," + score + ","
                        + metascore + "," + director;
                outputKey.set(movieName);
                outputValue.set(output);

                // Write out the key-value pair
                context.write(outputKey, outputValue);
            }
        } finally {
            scanner.close();
        }
    }
}
