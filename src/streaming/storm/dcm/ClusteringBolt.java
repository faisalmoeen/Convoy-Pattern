package streaming.storm.dcm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import base.Convoy;
import ca.pfv.spmf.patterns.cluster.Cluster;
import ca.pfv.spmf.patterns.cluster.DoubleArray;
import org.apache.commons.lang.ObjectUtils;
import utils.DBSCAN.DBSCANNlogN;
import utils.DBSCAN.MyDoubleArrayDBS;
import utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by faisal on 8/7/15.
 */
public class ClusteringBolt extends BaseRichBolt {
    OutputCollector _collector;
    DBSCANNlogN algo;
    int m;
    double e;
    List<DoubleArray> points;
    private List<Convoy> C;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        algo = new DBSCANNlogN();
        m = ((Double)(conf.get("m"))).intValue();
        e = (double)(conf.get("e"));
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            long t = (long) tuple.getLongByField("time");
//            System.out.println("time="+t);
            List<Double[]> doublesList = (List<Double[]>) tuple.getValueByField("points");
            points = Utils.toArrayDBS(doublesList);
            Values v = new Values();
            List<Cluster> clusters = null;
            try {
                clusters = algo.runAlgorithmOnArray(m, e, points);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            if (clusters != null && clusters.size() > 0) {
                C = Utils.clustersToConvoyList(clusters);
//            System.out.println(C.toString());
                v.add(t);
                v.add(C);
                _collector.emit(tuple, v);
            } else {
                v.add(t);
                v.add(C);
                _collector.emit(tuple, v);
            }
            _collector.ack(tuple);
        }
        catch (Exception e){
            if(e instanceof NullPointerException){
                NullPointerException np = (NullPointerException)e;
                np.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time","clusters"));
    }



}