import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class Demo {
	public static void main(String[] args) throws Exception {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("record_spout", new IRichSpout() {
			private static final long serialVersionUID = 9190932391734737990L;
			
			private SpoutOutputCollector collector;
			private TopologyContext context;

			private BufferedReader recordReader;

			@Override
			public Map<String, Object> getComponentConfiguration() {
				return null;
			}
			
			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("uid"));
			}
			
			@SuppressWarnings("rawtypes")
			@Override
			public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
				this.context = context;
				this.collector = collector;
				
				try {
					recordReader = new BufferedReader(new InputStreamReader(Runtime.getRuntime()
							.exec("cat /home/flying_xx/Documents/Workspace/Eclipse/storm_demo/sample/sample1")
							.getInputStream()));
				} catch (IOException e) {
					e.printStackTrace();
					recordReader = null;
				}
			}
			
			@Override
			public void nextTuple() {
				try {
					String record = recordReader.readLine();
					if (!StringUtils.isBlank(record)) {
						collector.emit(new Values(record));
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			@Override
			public void fail(Object arg0) {
			}
			
			@Override
			public void deactivate() {
			}
			
			@Override
			public void close() {
			}
			
			@Override
			public void activate() {
			}
			
			@Override
			public void ack(Object arg0) {
			}
		});
		
		topologyBuilder.setBolt("", new IBasicBolt() {
			private static final long serialVersionUID = -7169769496750959698L;
			
			private TopologyContext context;

			@Override
			public Map<String, Object> getComponentConfiguration() {
				return null;
			}
			
			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
//				declarer.declare(new Fields(""));
			}
			
			@SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
			public void prepare(Map conf, TopologyContext context) {
				conf.put(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY, "");
				this.context = context;
			}
			
			@Override
			public void execute(Tuple input, BasicOutputCollector collector) {
//				collector.emit(new Values());
			}
			
			@Override
			public void cleanup() {
			}
		}).shuffleGrouping("record_spout");
		
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_DEBUG, true);
//		conf.put(Config.TOPOLOGY_WORKERS, 10);
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("test", conf, topologyBuilder.createTopology());
		
		Thread.sleep(100000l);
	}
}
