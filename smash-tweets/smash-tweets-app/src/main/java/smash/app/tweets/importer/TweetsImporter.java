package smash.app.tweets.importer;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smash.data.tweets.pojo.Tweet;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.JobTimer;
import smash.utils.spark.FeatureRDDToGeoMesa;
import smash.utils.streamTasks.ml.sentiment.Sentiment;

import java.io.IOException;
import java.util.*;

/**
 * @author Yikai Gong
 */

public class TweetsImporter {
  private static Logger logger = LoggerFactory.getLogger(TweetsImporter.class);

  // Stanford NLP properties
  private static Properties props = new Properties();
  private static List<String> keepPOS = new ArrayList<>();
  private static StanfordCoreNLP pipeline;

  // Initiate properties required by NLP core
  static {
    props.setProperty("annotators",
      "tokenize, ssplit, pos, lemma, parse, sentiment");
    props.setProperty("tokenize.whitespace", "false");
    props.setProperty("tokenize.options", "untokenizable=noneKeep");
    props.setProperty("ssplit.tokenPatternsToDiscard", "allDelete");
    keepPOS.addAll(Arrays.asList("FW", "JJ", "JJR", "JJS", "NN",
      "NNS", "NNP", "NNPS", "RB", "RP", "VB", "VBD", "VBG", "VBN", "VBP",
      "VBZ", "WRB"));
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    TweetsImporterOptions options = new TweetsImporterOptions();
    options.parse(args);
    TweetsImporter importer = new TweetsImporter();
    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

  private SparkConf sparkConf;

  private TweetsImporter(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
    sparkConf.setAppName(this.getClass().getSimpleName());
    sparkConf.set("spark.files.maxPartitionBytes", "33554432"); // 32MB
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());

  }

  private TweetsImporter() {
    this(new SparkConf());
  }

  private void run(TweetsImporterOptions options) throws IOException {
    // Ensures Feature Type is saved in GeoMesa
    GeoMesaDataUtils.saveFeatureType(options, TweetsFeatureFactory.SFT);
    // Launch Spark Context
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      // Parse tweets json file to json string rdd
      JavaRDD<String> rawJson = sc.textFile(options.inputFile, 40);
      Dataset<Row> tweetRaw;
      tweetRaw = ss.read().json(rawJson).selectExpr("value.*");
//      tweetRaw.show();
      JavaRDD<String> tweetStr = tweetRaw.toJSON().toJavaRDD();
      // Parse tweets json rdd and then map to feature rdd
      JavaRDD<SimpleFeature> featureRDD = tweetStr.flatMap(json -> {
        List<SimpleFeature> tweets = new ArrayList<>();
        try {
          Tweet tweet = Tweet.fromJSON(json);
          if (tweet.isValid()) {
            tweet = processTweet(tweet);
          }
          if (tweet.isGeoTweet()) {
            SimpleFeature feature = TweetsFeatureFactory.createFeature(tweet);
            tweets.add(feature);
          }
        } catch (Exception e) {
          logger.warn(e.getMessage());
        }
        return tweets.iterator();
      });
      SpatialRDDProvider sp = GeoMesaSpark.apply(options.getAccumuloOptions2());
//      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
//      jsp.save(featureRDD, options.getAccumuloOptions(), TweetsFeatureFactory.FT_NAME);
//      Long numSaved = featureRDD.count();

      Long numSaved = FeatureRDDToGeoMesa.save(options, featureRDD, sc);
      FeatureRDDToGeoMesa.closeFeatureWriterOnSparkExecutors(sc);

//      System.out.println(sc.defaultParallelism());
//      sc.parallelize(new ArrayList<>()).foreachPartition(p -> {
//        System.out.println("Thread Name: " + Thread.currentThread().getName());
//        System.out.println("Thread ID: " + Thread.currentThread().getId());
//      });
      System.out.println("Ingested " + numSaved + " tweets");
    }
  }


  protected static Tweet processTweet(Tweet tweet) throws Exception {
    Annotation document = new Annotation(tweet.getText());

    if (TweetsImporter.pipeline == null) {
      TweetsImporter.pipeline = new StanfordCoreNLP(TweetsImporter.props);
    }
    // Runs all Annotators on this text
    TweetsImporter.pipeline.annotate(document);
    List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
    // Builds a dictionary made of all Parts of Speech contained in
    // TweetCruncher.keepPOS,
    // in passing, it computes the sentiment
    Set<String> lemmas = new HashSet<String>();
    String lemma;
    int longest = 0;
    int mainSentiment = 0;
    for (CoreMap sentence : sentences) {
      Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
      int sentenceLength = sentence.toString().length();
      if (sentenceLength > longest) {
        mainSentiment = RNNCoreAnnotations.getPredictedClass(tree);
        longest = sentenceLength;
      }
      for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
        String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
        if (TweetsImporter.keepPOS.contains(pos)) {
          lemma = token.lemma().toLowerCase();
          if (!lemmas.contains(lemma)) {
            lemmas.add(lemma);
          }
        }
      }
    }
    // Writes lemmas and sentiment in the output Tweet
    tweet.setSentiment(Sentiment.getSentiment(mainSentiment).getValue());
    if (tweet.getTokens() != null) {
      tweet.getTokens().addAll(lemmas);
    } else {
      List<String> tokens = new ArrayList<>();
      tokens.addAll(lemmas);
      tweet.setTokens(tokens);
    }
    return tweet;
  }
}
