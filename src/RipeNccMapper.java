

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RipeNccMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private LongWritable orig_Key = new LongWritable();
	private Text orig_value = new Text();
	private LongWritable trans_Key = new LongWritable();
	private Text trans_value = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		 String[] data = line.split("\\|");
		 if(!data[5].isEmpty() && !data[6].isEmpty())
		 {
		 String[] asn_list = data[6].split(" ");
		 String prefix = data[5];
		 String asn_originating = asn_list[asn_list.length - 1];
		 
			 if(asn_originating.contains("{") || asn_originating.contains("}")) 
			 {
				 String[] asn_orig_arr = asn_originating.replace("{", "").replace("}", "").trim().split(",");
				 for(String asn_orig : asn_orig_arr ) 
				 {
					 try {
						orig_Key.set(Long.parseLong(asn_orig.trim()));
						orig_value.set("O" + getPrefix(prefix) + " | " + prefix);			
						context.write(orig_Key, orig_value);
					 }
					 catch (Exception e) {
							System.out.println(e);
						}
				}
			 }
			 else
				{
					orig_Key.set(Long.parseLong(asn_originating.trim()));
					orig_value.set("O" + getPrefix(prefix) + " | " + prefix);			
					context.write(orig_Key, orig_value);
				}
		  if(asn_list.length > 1) {
			List<String> asn_Transiting = Arrays.asList(asn_list).subList(1, asn_list.length);
			for(String asn_transit : asn_Transiting) 
			{
				if(asn_transit.contains("{") || asn_transit.contains("}")) 
				{
					String[] asn_transit_arr = asn_transit.replace("{", "").replace("}", "").trim().split(",");
					for(String asn_trans : asn_transit_arr ) 
					{
						try {
							trans_Key.set(Long.parseLong(asn_trans.trim()));
							trans_value.set("T" + getPrefix(prefix) + " | " + prefix);			
							context.write(trans_Key, trans_value);
						}
						catch (Exception e) {
							System.out.println(e);
						}
					}
				}
				else
				{
					trans_Key = new LongWritable(Long.parseLong(asn_transit.trim()));
					trans_value = new Text("T" + getPrefix(prefix) + " | " + prefix);			
					context.write(trans_Key, trans_value);
				}
			
			}
		   }
    	 }
   }
	
	public static String getPrefix(String prefix) {
		if(prefix.contains(":"))  
			 return "6"; 
			 else 
			 return "4";
	}
	
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

}