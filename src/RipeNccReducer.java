import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RipeNccReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		Iterator<Text> prefix_values = values.iterator();
		
		String val_O4 = "";
		String val_T4 = "";
		String val_O6 = "";
		String val_T6 = "";
		String ot = "";
		
		try {
			while (prefix_values.hasNext()) {
				String prefix_val = prefix_values.next().toString();
				String[] prefix_Arr = prefix_val.split("\\|");
				String prefix_1 = prefix_Arr[2];
				ot = prefix_Arr[1];
				
				if(ot.trim().equals("O4")) {
					val_O4 = val_O4 + " " + prefix_1;
					}
				if(ot.trim().equals("O6")) {
					val_O6 = val_O6 + " " + prefix_1;
					}
				if(ot.trim().equals("T4")) {
					val_T4 = val_T4 + " " + prefix_1;
					}
				if(ot.trim().equals("T6")) {
					val_T6 = val_T6 + " " + prefix_1;
					}				
			} // end of while
			if(!val_O4.equals("")) {
				context.write(key, new Text("O4" + "|" + val_O4.trim()));
			}
			if(!val_O6.equals("")) {
				context.write(key, new Text("O6" + "|" + val_O6.trim()));
			}
			if(!val_T4.equals("")) {
				context.write(key, new Text("T4" + "|" + val_T4.trim()));
			}
			if(!val_T6.equals("")) {
				context.write(key, new Text("T6" + "|" + val_T6.trim()));
			}
			
		   } //end of try
		catch (Exception e) {
			System.out.println(key.toString() + e);
		}

		//totalWordCount.set(sum);
		// context.write(key, new IntWritable(sum));
		
	}
}