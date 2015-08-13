package streaming.storm.dcm;

/**
 * Created by faisal on 8/7/15.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import ca.pfv.spmf.patterns.cluster.DoubleArray;
import clustering.DbscanFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.*;


public class FileObjsSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(FileObjsSpout.class);
    boolean _isDistributed;
    StreamFileReader streamFileReader;
    List<Double[]> points;
    long t=0;
    long s=1; //sampling rate
    SpoutOutputCollector _collector;

    public FileObjsSpout() {
        this(true);
    }

    public FileObjsSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        s = (Long)conf.get("s");
        try {
            streamFileReader = new StreamFileReader(conf.get("inputFilePath").toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("Opened FileObjsSpout");
    }

    public void close() {

    }

    public void nextTuple() {
        points = streamFileReader.getNextPointsAsDoubleArray(t+=s);
        if(points==null){
                Utils.sleep(1000);
                return;
        }
        Values v = new Values(t);
        v.add(points);
        _collector.emit(v);
//        System.out.println("t="+t);
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time","points"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }
}