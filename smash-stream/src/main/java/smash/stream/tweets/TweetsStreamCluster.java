package smash.stream.tweets;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonSyntaxException;
import com.vividsolutions.jts.geom.Envelope;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.ReferencedEnvelope3D;
import org.joda.time.DateTime;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.data.tweets.pojo.Tweet;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaWriter;
import smash.utils.geomesa.GeoMesaOptions;
import smash.utils.spark.FeatureRDDToGeoMesa;
import smash.utils.streamTasks.ml.spatioTemporal.CellsPartitioner;
import smash.utils.streamTasks.ml.spatioTemporal.ClusterCell;
import smash.utils.streamTasks.ml.spatioTemporal.DbscanTask;
import smash.utils.streamTasks.ml.spatioTemporal.STObj;

import java.io.IOException;
import java.util.*;

/**
 * @author Yikai Gong
 */

public class TweetsStreamCluster {
  private static Logger logger = LoggerFactory.getLogger(TweetsStreamCluster.class);
  private SparkConf sparkConf;

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);
    TweetsStreamCluster importer = new TweetsStreamCluster();
    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

  public TweetsStreamCluster(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
    sparkConf.setAppName(this.getClass().getSimpleName());
    sparkConf.set("spark.files.maxPartitionBytes", "33554432"); // 32MB
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
    Class[] classes = new Class[]{STObj.class, Vector.class, ClusterCell.class};
    this.sparkConf.registerKryoClasses(classes);
  }

  public TweetsStreamCluster() {
    this(new SparkConf());
  }

  // TODO need new options for including kafka parameters
  public void run(GeoMesaOptions options) throws InterruptedException, IOException {
    // Running in the main process
    Map<String, String> kafkaParams = new HashMap<>();
    Set<String> topicsSet = new HashSet<>();
    kafkaParams.put("metadata.broker.list", "scats-1-interface:9092");
    kafkaParams.put("auto.offset.reset", "smallest");
    topicsSet.add("tweets");


    // Step 1. ensure table/schema has been created in DB
    GeoMesaDataUtils.saveFeatureType(options, TweetsFeatureFactory.SFT);

    // Step 2. Create SparkStreaming context and define the operations.
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

    JavaPairInputDStream<String, String> directKafkaStream =
      KafkaUtils.createDirectStream(
        ssc,
        String.class, String.class,
        StringDecoder.class, StringDecoder.class,
        kafkaParams, topicsSet
      );

    // For each data pair in stream: tuple[_1, _2]
    directKafkaStream.toJavaDStream().foreachRDD(tickRDD -> {
      // Block for each tick - Running in streaming-job-executor-0 process (The Driver)
//      if (tickRDD.isEmpty())
//        return;
      //Todo read next clusterID from DB before accepting the data stream
      //Todo Apply Datum?
      Long nxtCluId = 0L;
      // Min number of points to form a cluster
      Long minPts = 3L;
      Long maxPts = 100L;
      // Time-Spatial epsilon
      Long dist_time = 600000L; //10min in milli-sec
      Double dist_spatial = 0.1D; //0.1km
      Double spatioTemp_ratio = 0.1D / 600000; // km/milli-sec
      Double epsilon = calculate_epsilon(dist_spatial, dist_time, spatioTemp_ratio);
//      Envelope bbox = new Envelope(144.6240369, 145.317, -38.03535, -37.57470703);
      ReferencedEnvelope3D bbox = new ReferencedEnvelope3D(144.624, 145.624, -38.03535, -37.03535, 0, 0, null);

      // TODO may not need to parse into SimpleFeature before creating STObj
      // Phase-1: Use STObj as the generic object for social media points
      JavaPairRDD<String, STObj> incomeRDD = tickRDD.distinct().flatMapToPair(tuple -> {
        List<Tuple2<String, STObj>> flatted = new ArrayList<>();
        try {
          Tweet t = Tweet.fromJSON(tuple._2);
          if (t != null && t.getCoordinates() != null) {
            SimpleFeature sf = TweetsFeatureFactory.createFeature(t);
            Date ts = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
            STObj stObj = new STObj(sf, ts, null, null, t.toJSON());
            flatted.add(new Tuple2<>(stObj.getObjId(), stObj));
          }
        } catch (JsonSyntaxException ignored) {
          logger.warn(ignored.getMessage());
        }
        return flatted.iterator();
      });
      if (incomeRDD.isEmpty()) return;
      //      System.out.println("incomeSize: " + incomeRDD.count());

      // Phase-2: Get earliest timestamp of the data in this tick
      List<STObj> reducedBorderPoints = incomeRDD.aggregate(new ArrayList<STObj>(2), (list, t) -> {
        if (list.isEmpty()) {
          list.add(0, t._2);
          list.add(1, t._2);
        } else {
          if (t._2.getTimestamp().getTime() < list.get(0).getTimestamp().getTime())
            list.set(0, t._2);
          if (t._2.getTimestamp().getTime() > list.get(1).getTimestamp().getTime())
            list.set(1, t._2);
        }
        return list;
      }, (list1, list2) -> {
        if (list1.isEmpty())
          return list2;
        else if (list2.isEmpty())
          return list1;
        else {
          if (list2.get(0).getTimestamp().getTime() < list1.get(0).getTimestamp().getTime())
            list1.set(0, list2.get(0));
          if (list2.get(1).getTimestamp().getTime() > list1.get(1).getTimestamp().getTime())
            list1.set(1, list2.get(1));
          return list1;
        }
      });

      if (reducedBorderPoints.size() < 2 || reducedBorderPoints.get(0) == null || reducedBorderPoints.get(1) == null)
        return;
      // Query history data
      Long timeMinDis = covertToTimeDiff(epsilon, spatioTemp_ratio) * (minPts - 1);
      Date queryStartTime = new Date(reducedBorderPoints.get(0).getTimestamp().getTime() - timeMinDis);
      Date queryEndTime = new Date(reducedBorderPoints.get(1).getTimestamp().getTime() + timeMinDis);
//      System.out.println("Start: " + queryStartTime);
//      System.out.println("End: " + queryEndTime);

      Filter filter = null;
      try {
        filter = CQL.toFilter("created_at DURING " +
          (new DateTime(queryStartTime)).toString() + "/" +
          (new DateTime(queryEndTime)).toString()
        );
      } catch (CQLException e) {
        logger.warn(e.getMessage());
      }
      GeoMesaWriter writer =
        GeoMesaWriter.getThreadSingleton(options, TweetsFeatureFactory.FT_NAME);
      Iterator<SimpleFeature> sfItr = writer.read(filter);

//      Long hisSize = Integer.valueOf(Iterators.size(sfItr)).longValue();
//      System.out.println("itr size: " + hisSize);      //todo remove


      ArrayList<STObj> historyDataList = new ArrayList<>();
      sfItr.forEachRemaining(sf -> {
        String tweetId = TweetsFeatureFactory.getObjId(sf);
        if (tweetId != null) {
          Date ts = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
          String clusterId = (String) sf.getAttribute(TweetsFeatureFactory.CLUSTER_ID);
          String clusterLabel = (String) sf.getAttribute(TweetsFeatureFactory.CLUSTER_LABEL);
          Tweet tweet = TweetsFeatureFactory.fromSFtoPojo(sf);
          STObj stObj = new STObj(sf, ts, clusterId, clusterLabel, tweet.toJSON());
          stObj.setNewInput(false);
          historyDataList.add(stObj);
        }
      });

      JavaPairRDD<String, STObj> historyRDD = ssc.sparkContext()
        .parallelize(historyDataList).flatMapToPair(stObj -> {
          List<Tuple2<String, STObj>> flatted = new ArrayList<>();
          if (stObj != null && stObj.getObjId() != null)
            flatted.add(new Tuple2<>(stObj.getObjId(), stObj));
          return flatted.iterator();
        });

      // Phase-3: Union two RDDs and apply Fast-Partition
      JavaPairRDD<String, STObj> union = incomeRDD.union(historyRDD).reduceByKey((stObj1, stObj2) -> stObj1);
      //TODO Memory only could be better
      union.persist(StorageLevel.MEMORY_AND_DISK());
      List<Map.Entry<Vector<Double>, Boolean>> points = union.map(tuple -> Maps.immutableEntry(tuple._2.getCoordinates(), tuple._2.isNewInput())).collect();
      ClusterCell topCell = new ClusterCell("0", points, bbox);
      CellsPartitioner partitioner = new CellsPartitioner(topCell, 2 * epsilon, maxPts, minPts, epsilon);
      partitioner.doPartition();
      Map<String, ClusterCell> map = partitioner.getCellsMap();

      map.forEach((id, clusterCell) -> {
        System.out.println(id);
        System.out.println(clusterCell.getPoints());
        System.out.println(clusterCell.getFinalSize());
        System.out.println(clusterCell.getBbx());
        System.out.println("=============================");
      });

      // Phase-4: Broadcast cells information. Shuffle data and do local DBSCAN
      Broadcast<Map<String, ClusterCell>> b_cellsMap = ssc.sparkContext().broadcast(map);
      // <cellId or "NOISE", Iterable<STOjb>>
      JavaPairRDD<String, Iterable<STObj>> shuffledPairRDD = union.flatMapToPair(tuple -> {
        ArrayList<Tuple2<String, STObj>> result = new ArrayList<>();
        Vector<Double> coordinate = tuple._2.getCoordinates();
        Iterator<Map.Entry<String, ClusterCell>> cellItr = b_cellsMap.getValue().entrySet().iterator();
        while (cellItr.hasNext()) {
          Map.Entry<String, ClusterCell> entry = cellItr.next();
          if (entry.getValue().getBbx().contains(coordinate.get(0), coordinate.get(1))) {
            result.add(new Tuple2<>(entry.getValue().getCellId(), tuple._2));
            return result.iterator();
          }
        }
        // if points do not belong to any cells (which means they are must Noise points)
        result.add(new Tuple2<>("NOISE", tuple._2));
        return result.iterator();
      }).groupByKey();

      // <clusterId#objId or NOISE#objId , STObj>
      JavaPairRDD<String, STObj> localClusteredRDD = shuffledPairRDD.flatMapToPair(tuple -> {
        ArrayList<Tuple2<String, STObj>> result = new ArrayList<>();
        Map<String, STObj> toBeUpdated;
        if (!tuple._1.equals("NOISE")) {
          toBeUpdated = DbscanTask.localDBSCAN(tuple._2, epsilon, spatioTemp_ratio, minPts);
          toBeUpdated.forEach((id, STObj) -> {
            result.add(new Tuple2<>(id, STObj));
          });
        } else {
          tuple._2.forEach(stObj -> {
            result.add(new Tuple2<>("NOISE#" + stObj.getObjId(), stObj));
          });
        }
        return result.iterator();
      });

      //==================================================
      //      localClusteredRDD.persist(StorageLevel.MEMORY_ONLY());

      // Fork 1 <objId, List<stobj>> only non-noise data
      JavaPairRDD<String, ArrayList<STObj>> objId_STObj_pair = localClusteredRDD.flatMapToPair(tuple -> {
        ArrayList<Tuple2<String, STObj>> result = new ArrayList<>();
        String[] ids = tuple._1.split("#");
        if (!ids[0].equals("NOISE")) {
          result.add(new Tuple2<>(ids[1], tuple._2));
        }
        return result.iterator();
      }).aggregateByKey(new ArrayList<>(), (list, stObj) -> {
        list.add(stObj);
        return list;
      }, (list1, list2) -> {
        list1.addAll(list2);
        return list1;
      });

      JavaPairRDD<HashSet<String>, String> from_to_ClusterIdPair = objId_STObj_pair.flatMapToPair(tuple -> {
        ArrayList<Tuple2<HashSet<String>, String>> result = new ArrayList<>();
        ArrayList<STObj> stObjs = tuple._2;
        // find a core for this point
        STObj core = null;
        HashSet<String> set = new HashSet<>();
        for (STObj stObj : stObjs) {
          assert (tuple._1.equals(stObj.getObjId()));
          set.add(stObj.getClusterID());
          if (stObj.getClusterLabel() != null && stObj.getClusterLabel().equals(STObj.LABEL_CORE)) {
            core = stObj;
          }
        }
        // if the labels of this point are all Borders, check the number of these labels
        if (core == null) {
          // elect a new core if the number of labels > minPts
          if (stObjs.size() > minPts)
            core = stObjs.get(0);
        }
        // Now we find or elect a core for this point, point all clusterID to this its od
        if (core != null) {
//          for (String id : set) {
          //            String key = stObj.getClusterID();
          result.add(new Tuple2<>(set, core.getClusterID()));
//          }
        }
        // Means this point are Border of very limit number of clusters(<minPts), then a merge is not needed.
        else {
          //TODO should multiple cluster IDs be added to the same Border Point?
        }
        return result.iterator();
      });

//      JavaPairRDD<HashSet<String>, String> tmp = from_to_ClusterIdPair.mapToPair(tuple -> {
//        return new Tuple2<>(tuple._2._1, tuple._2._2);
//      }).distinct();

      ArrayList<Tuple2<HashSet<String>, String>> set_list = Lists.newArrayList(from_to_ClusterIdPair.collect());
      HashMap<String, String> newMap = new HashMap<>();
      while (set_list.size() > 0) {
        Tuple2<HashSet<String>, String> seed = set_list.remove(0);
        HashSet<String> seed_set = seed._1;
        String seed_id = seed._2;
        Iterator<Tuple2<HashSet<String>, String>> itr = set_list.iterator();
        ArrayList<Tuple2<HashSet<String>, String>> toRemove = new ArrayList<>();
        while (itr.hasNext()) {
          Tuple2<HashSet<String>, String> candidate = itr.next();
          HashSet<String> candidate_set = candidate._1;
          Set<String> intersection = new HashSet<>(candidate_set);
          intersection.retainAll(seed_set);
          if (intersection.size() > 0) {
            seed_set.addAll(candidate_set);
            toRemove.add(candidate);
          }
        }
        seed_set.forEach(oldId -> {
          newMap.put(oldId, seed_id);
        });
        set_list.removeAll(toRemove);
      }


      Broadcast<HashMap<String, String>> b_mergeMap = ssc.sparkContext().broadcast(newMap);

      // Fork 2 <STObj>
      JavaPairRDD<String, STObj> resultMap = localClusteredRDD.flatMapToPair(tuple -> {
        ArrayList<Tuple2<String, STObj>> result = new ArrayList<>();
        String objId = tuple._1.split("#")[1];
        result.add(new Tuple2<>(objId, tuple._2));
        return result.iterator();
      }).aggregateByKey(new ArrayList<STObj>(), (list, stObj) -> {
        list.add(stObj);
        return list;
      }, (list1, list2) -> {
        list1.addAll(list2);
        return list1;
      }).mapToPair(tuple -> {
        ArrayList<STObj> stObjs = tuple._2;
        STObj resultPoint = null;
        // find a core for this point
        STObj core = null;
        for (STObj stObj : stObjs) {
          if (stObj.getClusterLabel() != null && stObj.getClusterLabel().equals(STObj.LABEL_CORE)) {
            core = stObj;
            break;
          }
        }
        // if the labels of this point are all Borders, check the number of these labels
        if (core == null && stObjs.size() > minPts) {
          // elect a new core if the number of labels > minPts
          core = stObjs.get(0);
          core.setClusterLabel(STObj.LABEL_CORE);
        }
        // Means this point are Border of very limit number of clusters(<minPts), then a merge is not needed.
        if (core == null) {
          //TODO should multiple cluster IDs be added to the same Border Point?
          resultPoint = stObjs.get(0);
        } else
          resultPoint = core;

        String toClusterId = b_mergeMap.getValue().get(resultPoint.getClusterID());
        if (toClusterId != null)
          resultPoint.setClusterID(toClusterId);
        return new Tuple2<>(resultPoint.getObjId(), resultPoint);
      });


      JavaRDD<STObj> finalRDD = union.union(resultMap).reduceByKey((stObj1, stObj2) -> {
        if (stObj1.getClusterID() != null)
          return stObj1;
        else
          return stObj2;
      }).map(t -> {
        return t._2;
      });

      JavaRDD<SimpleFeature> finalSFRDD = finalRDD.map(stObj -> {
//        System.out.println(stObj.getJson());
        Tweet tweet = Tweet.fromJSON(stObj.getJson());
        SimpleFeature sf = TweetsFeatureFactory.createFeature(tweet);
//        sf.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
        sf.setAttribute(TweetsFeatureFactory.CLUSTER_ID, stObj.getClusterID());
        sf.setAttribute(TweetsFeatureFactory.CLUSTER_LABEL, stObj.getClusterLabel());
        return sf;
      });
      FeatureRDDToGeoMesa.save(options, finalSFRDD, ssc.sparkContext());
      FeatureRDDToGeoMesa.closeFeatureWriterOnSparkExecutors(ssc.sparkContext());

//      System.out.println(finalRDD.count());

//      finalMap.collect().forEach(tuple2->{
//        System.out.println(tuple2._1 + " : " + tuple2._2.getClusterID() + " : " + tuple2._2.getClusterLabel());
//      });
//      newMap.forEach((set, id)->{
//        System.out.println(Joiner.on("#").join(set) + " : " + id);
//      });


//      JavaPairRDD<HashSet<String>, String> tmp = from_to_ClusterIdPair.reduceByKey((t1, t2) -> {
//        HashSet<String> set = t1._1;
//        set.addAll(t2._1);
//        return new Tuple2<>(set, t1._2);
//      }).flatMapToPair(tuple -> {
//        ArrayList<Tuple2<String, Tuple2<HashSet<String>, String>>> result = new ArrayList<>();
//        HashSet<String> set = tuple._2._1;
//        String toClusterId = tuple._2._2;
//        set.forEach(clusterId->{
//          result.add(new Tuple2<>(clusterId, new Tuple2<>(set, toClusterId)));
//        });
//        return result.iterator();
//      }).reduceByKey((t1, t2) -> {
//        HashSet<String> set = t1._1;
//        set.addAll(t2._1);
//        return new Tuple2<>(set, t1._2);
//      }).mapToPair(tuple ->{
//        HashSet<String> set = tuple._2._1;
//        String toCId = tuple._2._2;
//        return new Tuple2<>(set, toCId);
//      }).distinct();

//      tmp.collect().forEach(t->{
//        HashSet<String> set = t._1;
//        String toCId = t._2;
//        System.out.println(Joiner.on("#").join(set) + " : " + toCId);
//      });

//      from_to_ClusterIdPair.collectAsMap().forEach((id, tuple)->{
//        Set<String> set = tuple._1;
//        String ids = Joiner.on("#").join(set);
//        System.out.println(id + " : " + ids + " : " + tuple._2);
//      });

//      localClusteredRDD.collectAsMap().forEach((uniqueId, stObj)->{
//        if(!uniqueId.split("#")[0].equals("NOISE"))
//          System.out.println("uniqueId: "+ uniqueId + " , " + "stobjID: " + stObj.getObjId());
//      });

//      objId_STObj_pair.collectAsMap().forEach((interId, ary)->{
//        System.out.println(interId + " : " + ary.size());
//      });


      // <objId, <clusterId, clusterId, ...>>
//      JavaPairRDD<String, ArrayList<String>> objId_clusterIds_pairRDD = localClusteredRDD.mapToPair(tuple -> {
//        String[] ids = tuple._1.split("#");
//        return new Tuple2<>(ids[0], ids[1]);
//      }).aggregateByKey(new ArrayList<>(), (list, clusterId) -> {
//          list.add(clusterId);
//          return list;
//        }, (list1, list2) -> {
//          list1.addAll(list2);
//          return list1;
//        }
//      );
//      JavaPairRDD<String, String> from_toClusterId_pair = objId_clusterIds_pairRDD.flatMapToPair(tuple -> {
//        ArrayList<Tuple2<String, String>> result = new ArrayList<>();
//        ArrayList<String> clusterIds = tuple._2;
//        // Filter out those clusterIds.size = 1
//        if (clusterIds == null || clusterIds.size() <= 1)
//          return result.iterator();
//        String newId = clusterIds.get(0);
//        clusterIds.forEach(oldId -> {
//          result.add(new Tuple2<>(oldId, newId));
//        });
//        return result.iterator();
//      });
//
//      JavaPairRDD<String, String> clusterid_stOjb_pairRDD = localClusteredRDD.mapToPair(tuple -> {
//        return null;
//      });


//


//      tuple2JavaRDD.foreach(tuple -> {
//        // Block for each data entry - Running in executors' threads
//
//        // Create map data to ingest
//        Tweet t = null;
//        //Filter
//        try {
//          t = Tweet.fromJSON(tuple._2);
//        } catch (JsonSyntaxException ignored) {
//          logger.warn(ignored.getMessage());
//        }
//        if (t == null || t.getCoordinates() == null){
//          return;
//        }
//        StreamTaskWriter<SimpleFeature> writer2 =
//          GeoMesaWriter.getThreadSingleton(options, TweetsFeatureFactory.FT_NAME);
//        // Sentiment Analysis
////        SentimentAnalysis<String, Map.Entry<Integer, List<String>>> sentTask =
////          SentimentAnalysis.getThreadSingleton(logger, null);
////        Map<String, String> input = new HashMap<>();
////        input.put(t.getId_str(), t.getText());
////        sentTask.doTask(input);
////        Map.Entry<Integer, List<String>> result = sentTask.getLastResult();
////        t.setSentiment(result.getKey());
////        t.setTokens(result.getValue());
//
//        // Clustering
////        Properties dbscanProp = new Properties();
////        dbscanProp.setProperty(DbscanTask.PROP_NxtCluID, "0");
////        dbscanProp.setProperty(DbscanTask.PROP_MinPTS, "3");
////        dbscanProp.setProperty(DbscanTask.PROP_DistTime, "3600000"); // 1 hour in milliseconds
////        dbscanProp.setProperty(DbscanTask.PROP_DistSpatial, "1"); // 1 km
////        dbscanProp.setProperty(DbscanTask.PROP_TimeSpatialRatio, "10");  // 10 m/s
////        dbscanProp.put(DbscanTask.PROP_DBAgent, writer2);
////        DbscanTask<Vector<Double>, Object> dbscanTask =
////          DbscanTask.getThreadSingleton(logger, dbscanProp);
////        Map<String, Vector<Double>> dbscan_input = new HashMap<>();
////        Vector<Double> lonLat = new Vector<>();
////        lonLat.add(0, t.getCoordinates().getLon().doubleValue());
////        lonLat.add(1, t.getCoordinates().getLat().doubleValue());
////        dbscan_input.put(t.getId_str(), lonLat);
////        dbscanTask.doTask(dbscan_input);
//
//
//        // Ingest
//        SimpleFeature sf = TweetsFeatureFactory.createFeature(t);
//        Map<String, SimpleFeature> toIngest = new HashMap<>();
//        toIngest.put(tuple._1, sf);
//        // Create ingest task
//        Properties p = new Properties();
////        StreamTaskWriter<SimpleFeature> writer =
////          GeoMesaWriter.getThreadSingleton(options, sf.getFeatureType().getTypeName());
//        p.put(SFIngestTask.PROP_WRITER, writer2);
//
//        SFIngestTask<SimpleFeature, Object> ingestTask = SFIngestTask.getThreadSingleton(logger, p);
//        // Execute task
//        ingestTask.doTask(toIngest);
////        System.out.println(t.getId_str());
////        System.out.println(Thread.currentThread().getName());
//      });
    });


    // Step 3. Start the SparkStreaming context
    ssc.sparkContext().setLogLevel("WARN");
    ssc.start();
    ssc.awaitTermination();
  }


  public static double calculate_epsilon(Double sp_d_km, Long temp_d_milSec, Double spatioTemporalRatio) {
    assert (sp_d_km != null && temp_d_milSec != null && spatioTemporalRatio != null);
    double kms_per_radian_mel = 87.944d;
    double d1 = sp_d_km;
    double d2 = temp_d_milSec * spatioTemporalRatio;
    return Math.sqrt((Math.pow(d1, 2) + Math.pow(d2, 2)) / Math.pow(kms_per_radian_mel, 2));
  }

  public static Long covertToTimeDiff(Double epsilon, Double spatioTemporalRatio) {
    assert (epsilon != null && spatioTemporalRatio != null);
    double kms_per_radian_mel = 87.944d;
    return Math.round((epsilon * kms_per_radian_mel / spatioTemporalRatio) + 1);
  }
}
