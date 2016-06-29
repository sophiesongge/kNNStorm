package storm.bolt;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import kNN.Element;
import kNN.ListElemS;
import kNN.ListResultR;
import kNN.RecordComparator;


public class LocalBolt implements IRichBolt{
	
	private OutputCollector collector;
	private static ArrayList<Element> R;
	private static ArrayList<Element> S;
	
	private static int k;
	private static int d;
	
	private int GenerationSize=50;
	private int currentGenerationSize=0;
	
	public LocalBolt(int k, int d){
		this.k = k;
		this.d= d;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		R = new ArrayList<Element>();
		S = new ArrayList<Element>();
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String setID = input.getStringByField("setID");
		Element elem = (Element) input.getValueByField("elem");
		if(setID.equals("R")){
			R.add(elem);
			currentGenerationSize++;
		}else{
			S.add(elem);
		}
		
		if(currentGenerationSize == GenerationSize){
			for(int i=0; i<R.size(); i++){
				ListResultR topK1R = topKForOneR(R.get(i), S);
				int id = topK1R.getId();
				collector.emit(new Values(String.valueOf(id), topK1R.getTopNeighbor()));
			}
		}
		currentGenerationSize = 0;
		R.clear();
		S.clear();
	}

	
	public static ArrayList<ListElemS> distRListS(Element r, ArrayList<Element> S){
		ArrayList<ListElemS> SWithDistance = new ArrayList<ListElemS>();
		for(int i=0; i<S.size(); i++){
			int sum = 0;
			for(int j=0; j<d; j++){
				float diff = Math.abs(r.getCoord()[j]-S.get(i).getCoord()[j]);
				float square = (float)Math.pow(diff, 2);
				sum += square;
			}
			float dist = (float) Math.sqrt(sum);
			ListElemS les = new ListElemS(S.get(i).getId(), S.get(i).getCoord(), dist);
			SWithDistance.add(les);
		}
		return SWithDistance;
	}
	
	public static ListResultR topKForOneR(Element r, ArrayList<Element> S){
		ArrayList<ListElemS> interm = distRListS(r, S);
		ListResultR kNNQueueR = new ListResultR(r.getId(), r.getCoord());
		RecordComparator rc = new RecordComparator();
		PriorityQueue<ListElemS> knnQueue = new PriorityQueue<ListElemS>(k + 1, rc);
		for(int i=0; i<interm.size(); i++){
			knnQueue.add(interm.get(i));
		}
		ArrayList<ListElemS> kNNQueue = new ArrayList<ListElemS>();
		for(int i=0; i<k; i++){
			kNNQueue.add(i, knnQueue.poll());
		}
		kNNQueueR.setTopNeighbor(kNNQueue);
		return kNNQueueR;
	}
	
	
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("rid","localneighbor"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
