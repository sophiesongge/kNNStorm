package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import storm.spout.kNNSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class kNNTopology{
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
			stormCall();
				
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
	
	
	public static void stormCall() throws InterruptedException
	{
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout_getdata", new kNNSpout(3, 2, 3),2);

		builder.setBolt("bolt_bloomfilter", new BoltCreatBF(),3).fieldsGrouping("spout_getdata", new Fields("Predicate"));

		builder.setBolt("bolt_test", new BoltTest(),1).shuffleGrouping("spout_getdata");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kNNStorm", config, builder.createTopology());
		Thread.sleep(10000);
		
		//Sander: cluster shutdown throws IOException, but adding try/catch states that it is an Unreachable catch block for IOException.
		try{
			cluster.shutdown();	
			throw new IOException("test");//Used as debug, otherwise we got the error saying this block couldn't generate an IOException
		} catch(IOException e){
			System.out.println("IOException when shutting down the cluster, continued afterwards, error message: " + e.getMessage());
		}
	}
}