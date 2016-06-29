package storm.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import storm.topology.kNNTopology;
import kNN.Element;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class kNNSpoutS extends BaseRichSpout{
	
	SpoutOutputCollector _collector;
	private Random r;
	private static int k;//the number of nearest neighbors
	private static int d;//dimension

	private int numberOfPartition;

	BufferedReader reader;
	String setID;


	
	public kNNSpoutS(int k, int d, int p, String setID){
		this.k = k;
		this.d= d;
		this.numberOfPartition = p;
		this.r = new Random();
		this.setID = setID;
	}
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this._collector = collector;
		this.reader = kNNTopology.readerS;
		
	}

	public void nextTuple() {
		Utils.sleep(200);
		generateTuple();
	}
	
	
	public void generateTuple(){
		
		try{
			String tempsString = null;
			while((tempsString = reader.readLine())!=null){
				String parts[] = tempsString.split(" +");
				int id = Integer.parseInt(parts[0]);
				float[] coord = new float[d];
				for(int ii=0; ii<d; ii++){
					try{
						coord[ii] = Float.valueOf(parts[1+ii]);
					}catch(NumberFormatException ex){
						//Do Nothing
					}
				}
				
				Element er = new Element(id, coord);
				er.setId(id);
				er.setCoord(coord);
				
				int partId = r.nextInt(numberOfPartition);
				int groupId = 0;
				
				for(int i=0; i<numberOfPartition; i++){
					groupId = partId + i*numberOfPartition;
					_collector.emit(new Values(er, groupId, setID));
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			System.out.println("Job is finished of this spout");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("elem", "ID", "setID"));
		
	}
	
}