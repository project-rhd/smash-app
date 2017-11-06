package smash.stream.tweets;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonSyntaxException;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.jts.ReferencedEnvelope3D;
import org.joda.time.DateTime;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
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
import java.util.stream.Collectors;

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
    this.sparkConf.set("spark.streaming.backpressure.enabled", "true");
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
//    kafkaParams.put("auto.offset.reset", "smallest");
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

    //Todo read next clusterID from DB before accepting the data stream
    //Todo Apply Datum?
    // Min number of points to form a cluster
    Long minPts = 3L;
    Long maxPts = 100L;
    // Time-Spatial epsilon
    Long dist_time = 600000L; //10min in milli-sec
    Double dist_spatial = 0.1D; //0.1km
    Double spatioTemp_ratio = 0.1D / 600000; // km/milli-sec
    Double epsilon = DbscanTask.get_STDistance_radian(dist_spatial, dist_time, spatioTemp_ratio);

    JavaSparkContext sc = JavaSparkContext.fromSparkContext(ssc.sparkContext().sc());
    SpatialRDDProvider sp = GeoMesaSpark.apply(options.getAccumuloOptions2());
    JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);

    // Step 3. For each data pair in stream: tuple[_1, _2]
    directKafkaStream.toJavaDStream().foreachRDD(tickRDD -> {
      // Block for each tick - Running in streaming-job-executor-0 process (The Driver)

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
      Long timeMinDis = DbscanTask.covertToTimeDiff(epsilon, spatioTemp_ratio) * (minPts);
      Date queryStartTime = new Date(reducedBorderPoints.get(0).getTimestamp().getTime() - timeMinDis);
      Date queryEndTime = new Date(reducedBorderPoints.get(1).getTimestamp().getTime() + timeMinDis);
      Filter filter = null;
      try {
        filter = CQL.toFilter("created_at DURING " +
          (new DateTime(queryStartTime)).toString() + "/" +
          (new DateTime(queryEndTime)).toString()
        );
      } catch (CQLException e) {
        logger.warn(e.getMessage());
      }

      JavaPairRDD<String, STObj> historyRDD = getSTObjRddFromGeoMesa(jsp, sc, options, filter);

      // Phase-3: Union two RDDs and apply Fast-Partition
      JavaPairRDD<String, STObj> union = incomeRDD.union(historyRDD)
        .reduceByKey((stObj1, stObj2) -> {
          if (!stObj1.isNewInput())
            return stObj1;
          else
            return stObj2;
        });

      //TODO Memory only could be better
//      union.persist(StorageLevel.MEMORY_AND_DISK());
      List<Map.Entry<Vector<Double>, Boolean>> points = union.map(tuple -> Maps.immutableEntry(tuple._2.getSTVector(spatioTemp_ratio), tuple._2.isNewInput())).collect();
      double min_t = DbscanTask.get_STDistance_radian(0d, queryStartTime.getTime(), spatioTemp_ratio);
      double max_t = DbscanTask.get_STDistance_radian(0d, queryEndTime.getTime(), spatioTemp_ratio);
      ReferencedEnvelope3D bbox = new ReferencedEnvelope3D(144.624, 145.624, -38.03535, -37.03535, min_t, max_t, null);
      ClusterCell topCell = new ClusterCell("000", points, bbox);
      CellsPartitioner partitioner = new CellsPartitioner(topCell, 2 * epsilon, maxPts, minPts, epsilon);
      partitioner.doPartition();
      Map<String, ClusterCell> map = partitioner.getCellsMap();

//      map.forEach((id, clusterCell) -> {
//        System.out.println(id + " size: " + clusterCell.getFinalSize());
//      });

      // Phase-4: Broadcast cells information. Shuffle data and do local DBSCAN
      Broadcast<Map<String, ClusterCell>> b_cellsMap = ssc.sparkContext().broadcast(map);
      // <cellId or "NOISE", Iterable<STOjb>>
      JavaPairRDD<String, Iterable<STObj>> shuffledPairRDD = union.flatMapToPair(tuple -> {
        ArrayList<Tuple2<String, STObj>> result = new ArrayList<>();
        Vector<Double> coordinate = tuple._2.getSTVector(spatioTemp_ratio);
        Iterator<Map.Entry<String, ClusterCell>> cellItr = b_cellsMap.getValue().entrySet().iterator();
        while (cellItr.hasNext()) {
          Map.Entry<String, ClusterCell> entry = cellItr.next();
          if (entry.getValue().getBbx().contains(coordinate.get(0), coordinate.get(1), coordinate.get(2))) {
            result.add(new Tuple2<>(entry.getValue().getCellId(), tuple._2));
          }
        }
        // if points do not belong to any cells (which means they are must Noise points)
//        if (result.isEmpty())
//          result.add(new Tuple2<>("NOISE", tuple._2));
        return result.iterator();
      }).groupByKey();

      // <clusterId#objId or NOISE#objId , STObj>
      JavaPairRDD<String, STObj> localClusteredRDD = shuffledPairRDD.flatMapToPair(tuple -> {
        ArrayList<Tuple2<String, STObj>> result = new ArrayList<>();
        ArrayList<STObj> toBeUpdated;
        if (!tuple._1.equals("NOISE")) {
          toBeUpdated = DbscanTask.localDBSCAN(tuple._2, epsilon, spatioTemp_ratio, minPts);
          toBeUpdated.forEach((stObj) -> {
            String id = stObj.getObjId();
            result.add(new Tuple2<>(id, stObj));
          });
        }
        return result.iterator();
      });

      //==================================================
      //      localClusteredRDD.persist(StorageLevel.MEMORY_ONLY());

      // Fork 1 <objId, List<stobj>> only non-noise data
      JavaPairRDD<String, ArrayList<STObj>> objId_stObjs = localClusteredRDD.aggregateByKey(new ArrayList<>(), (list, stObj) -> {
        list.add(stObj);
        return list;
      }, (list1, list2) -> {
        list1.addAll(list2);
        return list1;
      });

      JavaPairRDD<HashSet<String>, String> from_to_ClusterIdPair = objId_stObjs.flatMapToPair(tuple -> {
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
          result.add(new Tuple2<>(set, core.getClusterID()));
        }
        // Means this point are Border of very limit number of clusters(<minPts), then a merge is not needed.
        else {
          //TODO should multiple cluster IDs be added to the same Border Point?
        }
        return result.iterator();
      });

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
      JavaPairRDD<String, STObj> resultMap = objId_stObjs.mapToPair(tuple -> {
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
        assert (resultPoint.getClusterID() != null && !resultPoint.getClusterID().isEmpty());

        resultPoint.setToBeUpdated(true);
        return new Tuple2<>(resultPoint.getObjId(), resultPoint);
      });

      // Query data by clusterId
      JavaPairRDD<String, STObj> clusterPoints = null;
      if (newMap.isEmpty())
        clusterPoints = sc.emptyRDD().mapToPair(o-> new Tuple2<>(null, null));
      else{
        Set<String> clusterId_set = newMap.keySet();
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        List<Filter> match = new ArrayList<>();

        String clusterId = clusterId_set.iterator().next();
        Filter aMatch = ff.equals(ff.property(TweetsFeatureFactory.CLUSTER_ID), ff.literal(clusterId));
        match.add(aMatch);

//        for (String clusterId : clusterId_set) {
//          Filter aMatch = ff.equals(ff.property(TweetsFeatureFactory.CLUSTER_ID), ff.literal(clusterId));
//          match.add(aMatch);
//        }
        filter = ff.or(match);
//        Set<String> clusterId_set = newMap.keySet().stream().map(str -> "'" + str + "'").collect(Collectors.toSet());
//        String cqlStr = TweetsFeatureFactory.CLUSTER_ID + " in (" + Joiner.on(",").join(clusterId_set) + ")";
//        System.out.println("cql: " +cqlStr);
//        try {
//          filter = CQL.toFilter(cqlStr);
//        } catch (CQLException e) {
//          logger.warn(e.getMessage());
//        }
        clusterPoints = getSTObjRddFromGeoMesa(jsp, sc, options, filter);
      }

      JavaRDD<STObj> finalRDD = incomeRDD.union(clusterPoints).union(resultMap).reduceByKey((stObj1, stObj2) -> {
        if (stObj1.getToBeUpdated() && stObj1.getClusterID() != null)
          return stObj1;
        else
          return stObj2;
      }).map(t -> {
        STObj stObj = t._2;
        String toClusterId = b_mergeMap.getValue().get(stObj.getClusterID());
        if (toClusterId != null && !toClusterId.isEmpty())
          stObj.setClusterID(toClusterId);
        return t._2;
      });

      JavaRDD<SimpleFeature> finalSFRDD = finalRDD.map(stObj -> {
        Tweet tweet = Tweet.fromJSON(stObj.getJson());
        SimpleFeature sf = TweetsFeatureFactory.createFeature(tweet);
//        sf.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
        sf.setAttribute(TweetsFeatureFactory.CLUSTER_ID, stObj.getClusterID());
        sf.setAttribute(TweetsFeatureFactory.CLUSTER_LABEL, stObj.getClusterLabel());

        return sf;
      });
      FeatureRDDToGeoMesa.save(options, finalSFRDD, ssc.sparkContext());
      FeatureRDDToGeoMesa.closeFeatureWriterOnSparkExecutors(ssc.sparkContext());


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

  public static JavaPairRDD<String, STObj> getSTObjRddFromGeoMesa(JavaSpatialRDDProvider jsp, JavaSparkContext sc, GeoMesaOptions options, Filter filter) {
    return jsp.rdd(new Configuration(), sc, options.getAccumuloOptions(), new Query(TweetsFeatureFactory.FT_NAME, filter))
      .flatMapToPair(sf -> {
        List<Tuple2<String, STObj>> flatted = new ArrayList<>();
        String tweetId = TweetsFeatureFactory.getObjId(sf);
        if (tweetId != null) {
          Date ts = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
          String clusterId = (String) sf.getAttribute(TweetsFeatureFactory.CLUSTER_ID);
          String clusterLabel = (String) sf.getAttribute(TweetsFeatureFactory.CLUSTER_LABEL);
          Tweet tweet = TweetsFeatureFactory.fromSFtoPojo(sf);
          STObj stObj = new STObj(sf, ts, clusterId, clusterLabel, tweet.toJSON());
          stObj.setNewInput(false);
          if (stObj.getObjId() != null)
            flatted.add(new Tuple2<>(stObj.getObjId(), stObj));
        }
        return flatted.iterator();
      });
  }


}
