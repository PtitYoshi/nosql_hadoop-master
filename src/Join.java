import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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

public class Join {
	
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/Distinct-";
	private static final Logger LOG = Logger.getLogger(Join.class.getName());
	
	private static final String[][] KEY_ATTR = {{}};
	//Structure des "statics" IND : {{attrX1,attrX2,...},{attrY1,attrY2,...},...}
	private static final Integer[][] KEYS_IND = {{1},{}};
	
	private static final String[][] VAL_ATTR = {{}};
	private static final Integer[][] VALUES_IND = {{},{3}};
	private static final Class<?>[][] CLASS_VALUES = {{null},{Float.class}};
	
	//Structure de TABLES : {{"NOM_TABLE1",nbColumns},...}
	private static final String[][] TABLES = {{"CUSTOMERS","8"},{"ORDERS","9"}};
	private static final Integer[] JOIN_IND = {0,1};
	
	private static final String SPLITER="|";
	
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

		//La classe map s'applique sur chaque ligne de la totalité des fichiers.
		private HashMap<String, ArrayList<Integer>> keysInds = new HashMap<String, ArrayList<Integer>>();
		//private ArrayList<Integer> keysInd= new ArrayList<Integer>();
		private HashMap<String, ArrayList<Integer>> valsInds = new HashMap<String, ArrayList<Integer>>();
		//private ArrayList<Integer> valsInd=new ArrayList<Integer>();
		
		@SuppressWarnings("unlikely-arg-type")
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] values = value.toString().split("["+SPLITER+"]+");

			if(keysInds.isEmpty()&KEYS_IND.length>0)										//0. Si les indices sont vides mais que les statics IND sont remplit, alors on les initialise
				initialize(keysInds,KEYS_IND);
			if(valsInds.isEmpty()&VALUES_IND.length>0)
				initialize(valsInds,VALUES_IND);

			int table= identifyTable(values);											//Identifier la table
			if(table<0)
			{
				System.err.println("ERROR ! NO TABLE RENSEIGNED");
				return;
			}
			
			//if(!keysInds.isEmpty()|!valsInds.isEmpty())										//1. Si les indices de filtres sont connues, alors on ajoute les valeurs correspondantes
			//if(!indsEmpty(keysInds, valsInds, table))
			if(keysInds.containsKey(TABLES[table])|valsInds.containsKey(TABLES[table]))
			{

				String tmpKeys = findKeys(values, table);									//a. On complète Keys	
				String tmpVals = findValues(values, table);									//b. On complète Vals
								
				
				write(tmpKeys, tmpVals, values, table, context);							//c. On écrit
			}
			else 
			{
				if(KEY_ATTR[table].length>0)												//2. Sinon, on extrait les indices de filtres sur la première ligne.
					initializeByFirstColumn(values, table, keysInds, KEY_ATTR[table]);
				if(VAL_ATTR[table].length>0)
					initializeByFirstColumn(values, table, valsInds, VAL_ATTR[table]);
			}
				
			
		}

		private void initializeByFirstColumn(
				String[] values, int table, HashMap<String, ArrayList<Integer>> inds, String[] Attr)
		{
			inds.put(TABLES[table][0], new ArrayList<Integer>());
			for(int i=0;i<values.length;i++)
				for(String k : Attr)
					if(values[i].equals(k))
						inds.get(TABLES[table][0]).add(i);
		}

		private void write(String tmpKeys, String tmpVals, String[] values, int table, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException
		{
			if(JOIN_IND.length>0)
			{
				context.write(new Text(values[JOIN_IND[table]]),new Text("$"+table+tmpKeys+"&"+tmpVals));
			}
		}

		private String findValues(String[] values, int table)
		{
			String res = "";
			
			if(!valsInds.isEmpty()&valsInds.containsKey(TABLES[table][0]))
			{				
				for(int i=0;i<valsInds.get(TABLES[table][0]).size();i++)
				{
					if(CLASS_VALUES[table][i]==Object.class)
						res+="1";
					else
						res+=values[valsInds.get(TABLES[table][0]).get(i)];
					res+=SPLITER;
				}
			}
			return res;
		}

		private String findKeys(String[] values, int table)
		{
			String res="";
			if(!keysInds.isEmpty()&keysInds.containsKey(TABLES[table][0]))
			{
				for(int i=0;i<keysInds.get(TABLES[table][0]).size();i++)
				{
					res+=values[keysInds.get(TABLES[table][0]).get(i)]+SPLITER;
				}
			}

			
			return res;
		}

		private int identifyTable(String[] values)
		{
			int t = -1;
			for(int i=0;i<TABLES.length;i++)
			{
				//System.out.println(values.length+" = "+TABLES[i][1]+" ?");
				if(values.length==new Integer(TABLES[i][1]).intValue())
					t=i;
			}
			
			return t;
		}

		private void initialize(HashMap<String, ArrayList<Integer>> ArrayInd,
				Integer[][] StaticInd) 
		{
			
			int ind=0;
			for(Integer[] i:StaticInd)
			{
				ArrayInd.put(TABLES[ind][0], new ArrayList<Integer>());
				for(Integer j:i)
					ArrayInd.get(TABLES[ind][0]).add(j);
				ind++;
			}
		}
		
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			//Faire la jointure
			ArrayList<String> tmpContext=new ArrayList<String>();
			
			if(JOIN_IND.length>0)
				join(key, values, tmpContext);
			
			
			//Integrer au contexte
			if(!tmpContext.get(0).isEmpty()&!tmpContext.get(1).isEmpty())
				context.write(new Text(tmpContext.get(0)), new Text(tmpContext.get(1)));
			
			
		}

		private void join(Text key, Iterable<Text> values, ArrayList<String> tmpContext)
		{
			String[] tmp;
			String newKeys="";
			String newVals="";
			ArrayList<Serializable> numbVals = initializeVal();
			for(Text e:values)
			{
				//Séparer les clés des valeurs
				tmp=e.toString().split("([$][0-9]|&)");
				if(tmp.length>=2)
					newKeys+=tmp[1];
				if(tmp.length>=3)
				{
					increments(numbVals, tmp[2], new Integer(e.toString().substring(1, 2)));
					//newVals+=tmp[2];
				}
			}
			
			newVals=NumbToString(numbVals);
			
			
			tmpContext.add(newKeys);
			tmpContext.add(newVals);
		}

		private String NumbToString(ArrayList<Serializable> numbVals) {
			String res="";
			
			for(int i=0;i<numbVals.size();i++)
			{
				if(numbVals.get(i).getClass()==String.class || ((Number) numbVals.get(i)).intValue()!=0)
					res+=numbVals.get(i);
				res+=SPLITER;
			}
			if(res.split("["+SPLITER+"]").length==0)
				res="";
			
			return res;
		}

		private void increments(ArrayList<Serializable> newVals, String vals, Integer table)
		{
			int ind=0;
			String[] splitVals = vals.split("["+SPLITER+"]");
			
			for(String val : splitVals)
			{
				if(ind ==1)
					return;
				if(CLASS_VALUES[table][ind]==Integer.class||CLASS_VALUES[table][ind]==Object.class)
				{
					try
					{
						newVals.set(ind, ((Integer) newVals.get(ind)).intValue()+new Integer(val));
					}
					catch(NumberFormatException e)
					{
						System.err.println("WARNING ! Float value in column dedicated for Integer valuation : "+val);
						newVals.set(ind, ((Integer) newVals.get(ind)).intValue()+new Float(val).intValue());
					}
				}
				else if(CLASS_VALUES[table][ind]==Float.class)
				{
					newVals.set(ind, ((Float) newVals.get(ind)).floatValue()+new Float(val));
				}
				else if(CLASS_VALUES[table][ind]==Double.class)
				{
					newVals.set(ind, ((Double) newVals.get(ind)).doubleValue()+new Double(val));
				}
				else if(CLASS_VALUES[table][ind]==String.class)
				{
					newVals.set(ind, ((String) newVals.get(ind))+val);
				}
				{
					
				}
				ind++;
			}
			return;
		}

		private ArrayList<Serializable> initializeVal()
		{
			ArrayList<Serializable> res = new ArrayList<Serializable>();

			for(int i=0;i<VALUES_IND.length;i++)
			{
				for(int j=0;j<VALUES_IND[i].length;j++)
				{
					Class<?> c = CLASS_VALUES[i][j];
					if(c==Integer.class||c==Object.class)
					{
						res.add(new Integer(0));
					}
					else if(c==Float.class)
					{
						res.add(new Float(0));
					}
					else if(c==Double.class)
					{
						res.add(new Double(0));
					}
					else
					{
						res.add(new String());
					}
				}
			}
			
			return res;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Join");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}

}
