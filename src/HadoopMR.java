import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
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

public class HadoopMR {
	
	private static final String INPUT_PATH = "input-TAM/";
	private static final String OUTPUT_PATH = "output/HadoopMR-";
	private static final Logger LOG = Logger.getLogger(HadoopMR.class.getName());
	
	private static final String[][] KEY_ATTR = {{"stop_name","trip_headsign","departure_time"}};
	//Structure des "statics" IND : {{attrX1,attrX2,...},{attrY1,attrY2,...},...}
	private static final Integer[][] KEYS_IND = {};
	private static final String[][] KEYS_FILTER = {{null,null,"time/$::"}};
	
	private static final String[][] VAL_ATTR = {{"route_short_name","route_short_name"}};
	private static final Integer[][] VALUES_IND = {};
	private static final Class<?>[][] CLASS_VALUES = {{Integer.class,Integer.class}};
	private static final String[][] VALUES_FILTER = {{"groupInf/5","groupSup/4"}};
	
	//Structure de TABLES : {{"NOM_TABLE1",nbColumns},...}
	private static final String[][] TABLES = {{"TAM","9"}};
	private static final Integer[] JOIN_IND = {};
	
	private static final String SPLITER=";";
	
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
			String tab = TABLES[table][0];
			if(keysInds.containsKey(tab)|valsInds.containsKey(tab))
			{
				String tmpKeys = findKeys(values, table);										//a. On complète Keys
				String tmpVals = findValues(values, table);										//b. On complète Vals							
								
				if((tmpKeys!=null) & (tmpVals!=null))
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

		private String filterClear(String[] values, String[][] filters, HashMap<String,ArrayList<Integer>>inds, int table) {
			String res = "";
			
			if(!inds.isEmpty()&inds.containsKey(TABLES[table][0]))
			{
				for(int i=0;i<inds.get(TABLES[table][0]).size();i++)
				{
					if(filters[table][i]!=null)
					{
						String[] filter = filters[table][i].split("/");
						switch(filter[0])
						{
							case "content" :
							{
								if(!contentFilterClear(filter[1],values[i]))
									return null;
								else
									res+=values[i]+";";
								break;
							}
							case "groupContent" :
							{
								if(values[i]!="1")
									System.out.println("BUG!");
								if(!contentFilterClear(filter[1],values[i]))
									return null;
								else
									res+=filter[1]+";";
								break;
							}
							case "time" :
							{
								res+=convertTime(filter[1],values[i],"h:m:s");
								break;
							}
							case "inf" :
							{
								if(!infFilterClear(filter[1],values[i]))
									return null;
								else
									res+="1;";
								break;
							}
							case "sup" :
							{
								if(!supFilterClear(filter[1],values[i]))
									return null;
								else
									res+="1;";
								break;
							}
							case "groupInf" :
							{
								if(!infFilterClear(filter[1],values[i]))
									res+= "0;";
								else
									res+="1;";
								break;
							}
							case "groupSup" :
							{
								if(!supFilterClear(filter[1],values[i]))
									res+= "0;";
								else
									res+="1;";
								break;
							}
							default :
							{
								System.err.println("Filter type not renseigned : "+filter[0]);
							}
						}
					}
					else
						res+=values[i]+";";
				}
			}
			
			return res;
		}

		private String convertTime(String filter, String value, String replacer)
		{
			String res="";
			String[] tmpFilter=filter.split(":");
			String[] tmpVal = value.split(":");
			String[] tmpReplacer=replacer.split(":");
			
			for(int i=0; i<tmpFilter.length;i++)
			{
				if(tmpFilter[i].equals("$"))
					res+=tmpVal[i]+tmpReplacer[i];
			}
			
			return res;
		}

		private boolean supFilterClear(String filter, String value) {
			Double NumbFilter = new Double(filter);
			Double NumbValue = new Double(value);
			
			return NumbFilter<NumbValue;
		}

		private boolean infFilterClear(String filter, String value)
		{			
			Double NumbFilter = new Double(filter);
			Double NumbValue = new Double(value);
			
			return NumbFilter>NumbValue;
		}

		private boolean contentFilterClear(String filter, String value) {
			boolean res=true;
			
			//String[] content =  {"","",""};
			//Si le filter contient des $ : 3 possibilités : préfixes, suffixes, et contains
			if(filter.contains("$"))
			{
				String[] content = filter.split("[$]");
				if(content.length>0)
				{
					res=value.startsWith(content[0]);
					if(res&content.length==2)
						res=value.endsWith(content[1]);
					if(res&content.length==3)
					{
						res=value.contains(content[1])&value.endsWith(content[2]);
					}
				}
					
			}
			else
			{
				res = value.equals(filter);
			}
			
			return res;
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
			else
				context.write(new Text(tmpKeys), new Text(table+"&"+tmpVals));
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
			return filterClear(res.split("["+SPLITER+"]+"),VALUES_FILTER, valsInds, table);
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

			
			return filterClear(res.split("["+SPLITER+"]+"),KEYS_FILTER, keysInds, table);
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
		
		/**
		 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
		 * Utilisé pour conserver les k mots les plus fréquents.
		 * 
		 * Il associe une fréquence à une liste de mots.
		 */
		private TreeMap<Serializable, List<Text>> sortedWords = new TreeMap<>();
		private int nbsortedWords = 0;
		private int k=0;
		private int sortedTable=0;
		
		@Override
		public void setup(Context context)
		{
			//k = context.getConfiguration().getInt("k", 1);
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			//Faire la jointure
			ArrayList<String> tmpContext=new ArrayList<String>();
			ArrayList<Serializable> numbVals = initializeVal(VAL_ATTR);
			
			if(JOIN_IND.length>0)
			{
				join(key, values, tmpContext, numbVals);
				
				//Integrer au contexte
				if(!tmpContext.get(0).isEmpty()&!tmpContext.get(1).isEmpty())
					context.write(new Text(tmpContext.get(0)), new Text(tmpContext.get(1)));
			}
			else
			{
				String newVals="";
				for(Text e:values)
				{
					String[] elem = e.toString().split("&");
					increments(numbVals,elem[1],new Integer(elem[0]));
				}
				if(k>0)
				{
					sort(key,numbVals);
				}
				else
				{
					newVals=NumbToString(numbVals);
					context.write(key,new Text(newVals));
				}
			}
			
			
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			Integer[] nbofs = sortedWords.keySet().toArray(new Integer[0]);

			// Parcours en sens inverse pour obtenir un ordre descendant
			int i = nbofs.length;

			while (i-- != 0) {
				Serializable nbof = nbofs[i];

				for (Text words : sortedWords.get(nbof))
					context.write(words, new Text(nbof.toString()));
			}
		}

		private void sort(Text key, ArrayList<Serializable> numbVals)
		{
			// On copie car l'objet key reste le même entre chaque appel du reducer
			Text keyCopy = new Text(key);

			// Fréquence déjà présente
			Serializable sum = numbVals.get(sortedTable);
			
			if (sortedWords.containsKey(sum))
				sortedWords.get(sum).add(keyCopy);
			else {
				List<Text> words = new ArrayList<>();

				words.add(keyCopy);
				sortedWords.put(sum, words);
			}

			// Nombre de mots enregistrés atteint : on supprime le mot le moins fréquent (le premier dans sortedWords)
			if (nbsortedWords == k) {
				Serializable firstKey = sortedWords.firstKey();
				List<Text> words = sortedWords.get(firstKey);
				words.remove(words.size() - 1);

				if (words.isEmpty())
					sortedWords.remove(firstKey);
			} else
				nbsortedWords++;			
		}

		private void join(Text key, Iterable<Text> values, ArrayList<String> tmpContext, ArrayList<Serializable> numbVals)
		{
			String[] tmp;
			String newKeys="";
			String newVals="";
			//ArrayList<Serializable> numbVals = initializeVal();
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
				{
					res+=numbVals.get(i);
				}
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

		private ArrayList<Serializable> initializeVal(Object[][] VALS)
		{
			ArrayList<Serializable> res = new ArrayList<Serializable>();

			for(int i=0;i<VALS.length;i++)
			{
				for(int j=0;j<VALS[i].length;j++)
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

		Job job = new Job(conf, "Hadoop");

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
