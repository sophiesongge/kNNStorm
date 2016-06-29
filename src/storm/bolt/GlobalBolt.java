package storm.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import kNN.ListElemS;
import kNN.RecordComparator;

public class GlobalBolt implements IRichBolt{
	
	private OutputCollector collector;
	private static Map result;
	private int k;
	private int GenerationSize=30;

	public GlobalBolt(int k){
		this.k = k;
	}
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.result = new HashMap();	
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		if(result.size()>GenerationSize){
			result.clear();
		}
		String rid = input.getStringByField("rid");
		ArrayList<ListElemS> localneighbor = (ArrayList<ListElemS>) input.getValueByField("localneighbor");
		
		if(!result.containsKey(rid)){
			RecordComparator rc = new RecordComparator();
			PriorityQueue<ListElemS> global = new PriorityQueue<ListElemS>(k + 1, rc);
			for(int i=0; i<localneighbor.size(); i++){
				if(localneighbor.get(i)!=null){
					global.add(localneighbor.get(i));	
				}
			}
			result.put(rid, global);
		}else{
			RecordComparator rc = new RecordComparator();
			PriorityQueue<ListElemS> global = new PriorityQueue<ListElemS>(k + 1, rc);
			global = (PriorityQueue<ListElemS>) result.get(rid);
			//ArrayList<ListElemS> temps = PriorityQueue<ListElem> result.get(rid);
			for(int i=0; i<localneighbor.size(); i++){
				if(localneighbor.get(i)!=null){
					global.add(localneighbor.get(i));	
				}
			}
			result.put(rid, global);
		}
		collector.emit(new Values(rid, result.get(rid)));
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		System.out.println("One Generation Finished");
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}