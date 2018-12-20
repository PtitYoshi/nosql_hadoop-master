
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupBy {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/groupBy-";
	private static final Logger LOG = Logger.getLogger(GroupBy.class.getName());
	
	//private static final String REGEX_TO_SPLIT = ",\n";
	private static final String[] KEY_ATTR = {"Customer Name"};
	private static final String[] VAL_ATTR = {"Customer Name"};
	private static final Class<?>[] CLASS_VALUES = {String.class};

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		//@Override
		//private final static IntWritable one = new IntWritable(1);				//La valeur d'une occurence.
		//private final static String emptyWords[] = { "" };						//Le mot vide.
		//private ArrayList<String> table = new ArrayList<String>();				//Table des données extraites du texte.
		private ArrayList<Integer> keysInd= new ArrayList<Integer>();
		private ArrayList<Integer> valsInd=new ArrayList<Integer>();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] values = value.toString().split("[,]+");
			
			if(!keysInd.isEmpty() & !valsInd.isEmpty())
			{
				//a. On compète Keys
				String tmpText = "";
				for(int i=0;i<keysInd.size();i++)
				{
					tmpText+=values[keysInd.get(i)];
					if(i<keysInd.size()-1)
						tmpText+=";";
				}
				//b. On complète Vals
				String tmpVals = "";
				for(int i=0;i<valsInd.size();i++)
				{
					if(CLASS_VALUES[i]==String.class)
						tmpVals+="1";
					else
						tmpVals+=values[valsInd.get(i)];
					if(i<valsInd.size()-1)
						tmpVals+=";";
				}
				
				//c. On écrit
				context.write(new Text(tmpText), new Text(tmpVals));
			}
			//2- Sinon, c'est qu'on est sur la premiere ligne, et qu'on veut extraire les indices			
			else
			{
				for(int i=0;i<values.length;i++)
				{
					for(String k : KEY_ATTR)
					{
						if(values[i].equals(k))
							keysInd.add(i);
					}
					for(String k : VAL_ATTR)
					{
						if(values[i].equals(k))
							valsInd.add(i);
					}
					
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//ArrayList<Float> sums = new ArrayList<Float>();
			ArrayList<Number> sums = new ArrayList<Number>();
			
			if(sums.isEmpty())
			{
				for(int i=0;i<VAL_ATTR.length;i++)
				{
					//sums.add(new Float(0));
					if(CLASS_VALUES[i]==Integer.class||CLASS_VALUES[i]==String.class)
					{
						sums.add(new Integer(0));
					}
					else if(CLASS_VALUES[i]==Float.class)
					{
						sums.add(new Float(0));
					}
					else if(CLASS_VALUES[i]==Double.class)
					{
						sums.add(new Double(0));
					}
				}
			}
			
			for(Text vals : values)
			{
				int ind=0;
				String[] splitVals = vals.toString().split("[&]");
				
				for(String val : splitVals)
				{
					//sums.set(ind, sums.get(ind)+new Float(val));
					
					if(CLASS_VALUES[ind]==Integer.class||CLASS_VALUES[ind]==String.class)
					{
						try
						{
							sums.set(ind, sums.get(ind).intValue()+new Integer(val));
						}
						catch(NumberFormatException e)
						{
							System.err.println("WARNING ! Float value in column dedicated for Integer valuation : "+val);
							sums.set(ind, sums.get(ind).intValue()+new Float(val).intValue());
						}
					}
					else if(CLASS_VALUES[ind]==Float.class)
					{
						sums.set(ind, sums.get(ind).floatValue()+new Float(val));
					}
					else if(CLASS_VALUES[ind]==Double.class)
					{
						sums.set(ind, sums.get(ind).doubleValue()+new Double(val));
					}
					ind++;
				}
			}
			
			String tmpText="";
			
			for(int i=0;i<sums.size();i++)
			{
				tmpText+=sums.get(i);
				if(i<sums.size()-1)
					tmpText+="\t\t";
			}
			
			if(KEY_ATTR[0]==VAL_ATTR[0])
				context.write(key, new Text(""));
			else
				context.write(key, new Text(tmpText));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(Text.class); 

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}