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

public class kNNSpoutOld extends BaseRichSpout{
	
	SpoutOutputCollector _collector;
	private Random r;
	//private FileReader fileReader;
	private static int k;//the number of nearest neighbors
	private static int d;//dimension
	//static String filePathR; //the file path for data set R
	//static String filePathS; //the file path for data set S
	private int numberOfPartition;
	//private int recIdOffset;
	//private int coordOffset;
	BufferedReader readerR;
	BufferedReader readerS;

	
	public kNNSpoutOld(int k, int d, int p){
		this.k = k;
		this.d= d;
		this.numberOfPartition = p;
		this.r = new Random();
	}
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this._collector = collector;
		//this.readerR = kNNTopology.readerR;
		//this.readerS = kNNTopology.readerS;
		
	}

	public void nextTuple() {
		Utils.sleep(10);
		generateTuple();
	}
	
	
	public void generateTuple(){
		
		try{
			String tempsStringR = null;
			while((tempsStringR = readerR.readLine())!=null){
				String parts[] = tempsStringR.split(" +");
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
					groupId = partId * numberOfPartition + i;
					_collector.emit(new Values(er, groupId));
				}
			}
			String tempsStringS = null;
			while((tempsStringS = readerS.readLine())!=null){
				String parts[] = tempsStringS.split(" +");
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
					groupId = partId + numberOfPartition * i;
					_collector.emit(new Values(er, groupId));
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
			//Utils.sleep(10000);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("elem", "ID"));
		
	}
	
	
}