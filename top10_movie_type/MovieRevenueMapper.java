// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.NullWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;

// import java.io.IOException;
// import java.util.TreeMap;

// public class MovieRevenueMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
//     private TreeMap<Long, Text> top10 = new TreeMap<Long, Text>();

// @Override
// public void map(LongWritable key, Text value, Context context) throws
// IOException, InterruptedException {
// // Skip header
// if (key.get() == 0 && value.toString().contains("Movie Name"))
// return;

// String line = value.toString();
// String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

// try {
// String movieName = parts[0].trim(); // Just trim, as we assume no brackets in
// cleaned data
// long revenue = (long) (Double.parseDouble(parts[3].trim()) * 1000000); //
// Revenue is now at index 3

// // Add movie name and revenue to our map with the revenue as the key
// top10.put(revenue, new Text(movieName + "," + revenue));

// // If we have more than ten records, remove the one with the lowest revenue
// if (top10.size() > 10) {
// top10.pollFirstEntry(); // Correct method to remove the lowest key
// }

// } catch (NumberFormatException e) {
// // Handle parse error or ignore the line
// }
// }

//     @Override
//     public void cleanup(Context context) throws IOException, InterruptedException {
//         // Output our top ten to the reducers with a null key
//         for (Text t : top10.values()) {
//             context.write(NullWritable.get(), t);
//         }
//     }
// }

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.TreeMap;

// public class MovieRevenueMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
//     private TreeMap<Long, Text> top10 = new TreeMap<Long, Text>();

//     @Override
//     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//         // Skip header
//         if (key.get() == 0 && value.toString().contains("Movie Name"))
//             return;

//         String line = value.toString();
//         String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

//         try {
//             String movieName = parts[0].trim(); // Just trim, as we assume no brackets in cleaned data
//             String movieDate = parts[1].trim().replaceAll("[()]", ""); // Remove parentheses from the movie date
//             long revenue = (long) (Double.parseDouble(parts[3].trim()) * 1000000); // Revenue is now at index 3

//             // Combine movie name, cleaned date, and revenue into a single output
//             Text output = new Text(movieName + "," + movieDate + "," + revenue);

//             // Add to our map with the revenue as the key
//             top10.put(revenue, output);

//             // If we have more than ten records, remove the one with the lowest revenue
//             if (top10.size() > 10) {
//                 top10.pollFirstEntry(); // Correct method to remove the lowest key
//             }

//         } catch (NumberFormatException e) {
//             // Handle parse error or ignore the line
//             System.err.println("Error parsing line: " + line);
//             e.printStackTrace();
//         }
//     }

//     @Override
//     public void cleanup(Context context) throws IOException, InterruptedException {
//         // Output our top ten to the reducers with a null key
//         for (Text t : top10.values()) {
//             context.write(NullWritable.get(), t);
//         }
//     }
// }
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.TreeMap;

public class MovieRevenueMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private TreeMap<Long, Text> top10 = new TreeMap<Long, Text>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header
        if (key.get() == 0 && value.toString().contains("Movie Name"))
            return;

        String line = value.toString();
        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        try {
            String movieName = parts[0].trim(); // Just trim, as we assume no brackets in cleaned data
            String movieDate = parts[1].trim().replace("(", "").replace(")", ""); // Remove parentheses from the movie
                                                                                  // date
            long revenue = (long) (Double.parseDouble(parts[3].trim()) * 1000000); // Revenue is now at index 3

            // Combine movie name, cleaned date, and revenue into a single output
            Text output = new Text(movieName + "," + movieDate + "," + revenue);

            // Add to our map with the revenue as the key
            top10.put(revenue, output);

            // If we have more than ten records, remove the one with the lowest revenue
            if (top10.size() > 10) {
                top10.pollFirstEntry(); // Correct method to remove the lowest key
            }

        } catch (NumberFormatException e) {
            // Handle parse error or ignore the line
            System.err.println("Error parsing line: " + line);
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // Output our top ten to the reducers with a null key
        for (Text t : top10.values()) {
            context.write(NullWritable.get(), t);
        }
    }
}
