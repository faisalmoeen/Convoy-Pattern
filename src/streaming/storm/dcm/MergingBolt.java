package streaming.storm.dcm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import base.Convoy;
import clustering.PointWrapper;
import org.apache.avro.generic.GenericData;
import org.apache.commons.math3.ml.clustering.Cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by faisal on 8/7/15.
 */
public class MergingBolt extends BaseRichBolt{
    OutputCollector _collector;
    long tPrevious=-0;
    HashMap<Long,List<Convoy>> map;
    long s=1; //sampling rate

    int m;
    double e;
    long t;
    long k;
    long startTime;

    List<Convoy> Vnext = new ArrayList<Convoy>();
    List<Convoy> V = new ArrayList<Convoy>();
    List<Convoy> Vpcc = new ArrayList<Convoy>();
    List<Convoy> C;
    List<Cluster<PointWrapper>> CC = null;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        s = (Long)conf.get("s");
        map = new HashMap<>();
        m = ((Double)(conf.get("m"))).intValue();
        e = (double)(conf.get("e"));
        k = (long)(conf.get("k"));
        startTime = (long)(conf.get("startTime"));
    }

    @Override
    public void execute(Tuple tuple) {
//            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
        t = tuple.getLongByField("time");
        C = (List<Convoy>) tuple.getValueByField("clusters");
//        System.out.println("received at merge: "+t);
        if(t!=tPrevious+s){
            map.put(t,C);
            return;
        }
        else{
            mergeStep();
            tPrevious=t;
            while (map.containsKey(tPrevious+s)){
                t=tPrevious+s;
                mergeStep();
                tPrevious=t;
            }
        }
//        System.out.println(C);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("convoy"));
    }

    private void mergeStep(){
        Vnext=new ArrayList<Convoy>();
        if(C==null){
            C = new ArrayList<Convoy>();
        }
        for(Convoy c : C){
            c.setMatched(false);c.setAbsorbed(false);
//				System.out.println(c);
        }
        for(Convoy v : V){
            v.setExtended(false);v.setAbsorbed(false);
            for(Convoy c : C){
                if(c.size()>=m && v.intersection(c).size() >= m){ //extend (v,c)
                    v.setExtended(true);c.setMatched(true);
                    Convoy vext = new Convoy(v.intersection(c),v.getStartTime(),t+s-1);
                    Vnext = updateVnext(Vnext, vext);
                    if(v.isSubset(c)){v.setAbsorbed(true);}
                    if(c.isSubset(v)){c.setAbsorbed(true);}
                }
            }
            if(!v.isAbsorbed()){
                if(v.lifetime() >= k){
                    Vpcc.add(v);
                }
            }
        }
        for(Convoy c : C){
            if(!c.isAbsorbed()){
                c.setStartTime(t-s+1);
                c.setEndTime(t+s-1);
                Vnext=updateVnext(Vnext, c);
            }
        }
        V=Vnext;
//        t=t+s;
        if(t>=2874){
            for(Convoy v:V){
                if(v.lifetime()>=k){
                    Vpcc.add(v);
                }
            }
            System.out.println("End Time = " + System.currentTimeMillis());
            System.out.println("Total Time = " + (System.currentTimeMillis()-startTime));
            for(Convoy v:Vpcc){
                System.out.println(v);
            }
            System.out.println("Total Convoys = "+Vpcc.size());
            Utils.sleep(10000);
        }
//			System.out.println("ts = "+t+", |V| = "+V.size()+", |Vpcc| = "+Vpcc.size());
//			System.out.println("No. of clusters = "+clusters.size());
        //algo.printStatistics();

    }

    public static List<Convoy> updateVnext(List<Convoy> Vnext, Convoy vnew){
        boolean added=false;
        for(Convoy v : Vnext){
            if(v.hasSameObjs(vnew)){
                if(v.getStartTime()>vnew.getStartTime()){//v is a subconvoy of vnew
                    Vnext.remove(v);
                    Vnext.add(vnew);
                    added=true;
                }
                else if(vnew.getStartTime()>v.getStartTime()){//vnew is a subconvoy of v *****different from vcoda
                    added=true;
                }
            }
        }
        if(added==false){
            Vnext.add(vnew);
        }
        return Vnext;
    }

}
