package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;

import storm.bolt.LocalBolt;
import storm.bolt.GlobalBolt;
import storm.spout.kNNSpout;
import storm.spout.kNNSpoutR;
import storm.spout.kNNSpoutS;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class kNNTopology implements Serializable {
	public static BufferedReader readerR;
	public static BufferedReader readerS;
	
	public static void main(String[] args) throws Exception{
		String filePathR = "./data/databig1.txt";
		String filePathS = "./data/databig2.txt";
		File fileR = new File(filePathR);
		File fileS = new File(filePathS);
		readerR = null;
		readerS = null;
		try{
			readerR = new BufferedReader(new FileReader(fileR));
			readerS = new BufferedReader(new FileReader(fileS));
			stormCall(args);
				
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(readerR != null){
				try{
					readerR.close();
				}catch(IOException e1){
					//Do nothing
				}
			}
			if(readerS != null){
				try{
					readerS.close();
				}catch(IOException e2){
					//Do nothing
				}
			}
		}
	}
	
	
	public static void stormCall(String[] args) throws InterruptedException
	{
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("getR", new kNNSpoutR(5, 2, 3, "R"), 1);
		
		builder.setSpout("getS", new kNNSpoutS(5, 2, 3, "S"), 1);
		
		builder.setBolt("local", new LocalBolt(5, 2), 9).fieldsGrouping("getR", new Fields("ID")).fieldsGrouping("getS", new Fields("ID"));
		
		builder.setBolt("global", new GlobalBolt(5), 3).fieldsGrouping("local", new Fields("rid"));

		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("kNNStorm", config, builder.createTopology());
		
		Thread.sleep(10000);
		
		try{
			cluster.shutdown();	
			throw new IOException("test");
		} catch(IOException e){
			System.out.println("IOException when shutting down the cluster, continued afterwards, error message: " + e.getMessage());
		}
	}
}