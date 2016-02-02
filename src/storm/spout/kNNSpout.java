package storm.spout;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import kNN.Element;
import storm.rdf.RDFTriple;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class kNNSpout extends BaseRichSpout{
	
	SpoutOutputCollector _collector;
	Random _rand;
	private FileReader fileReader;
	private static int k;//the number of nearest neighbors
	private static int d;//dimension
	static String filePathR; //the file path for data set R
	static String filePathS; //the file path for data set S
	
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this._collector = collector;
		this._rand = new Random();
		// TODO Auto-generated method stub
		
	}

	public void nextTuple() {
		Utils.sleep(100);
		generateTuple();
		
	}
	
	public void generateTuple(){
		
		File file = new File(filePathR);
		BufferedReader reader = null;
		ArrayList<Element> readList = new ArrayList<Element>();
		try{
			reader = new BufferedReader(new FileReader(file));
			String tempsString = null;
			while((tempsString = reader.readLine())!=null){
				String parts[] = tempsString.split(" +");
				int id= Integer.parseInt(parts[0]);
				/*System.out.print("id: "+parts[0]);
				System.out.print(" x: "+parts[0+1]);
				System.out.println(" y: "+parts[1+1]);*/				
				float[] coord = new float[d];
				for(int ii=0; ii<d; ii++){
					try{
						coord[ii] = Float.valueOf(parts[1+ii]);
					}catch(NumberFormatException ex){
						
					}
				}
				Element le = new Element(id, coord);
				le.setId(id);
				le.setCoord(coord);
				readList.add(le);
			}
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(reader != null){
				try{
					reader.close();
				}catch(IOException e1){
					//Do nothing
				}
			}
		}
		
		
		try{
			String tempsString = null;
			while((tempsString = _reader.readLine())!=null){
				String parts[] = tempsString.split(" +");
				String Subject = parts[0];				
				String Predicate = parts[1];
				String Object = parts[2];
				
				RDFTriple rdf = new RDFTriple(Subject, Predicate, Object);
				rdf.setSubject(Subject);
				rdf.setPredicate(Predicate);
				rdf.setObject(Object);
				
				//emit every line, Values is an instance of ArrayList
				_collector.emit(new Values(Subject, Predicate, Object));
				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			System.out.println("Job is finished of this spout");
			Utils.sleep(10000);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	
}