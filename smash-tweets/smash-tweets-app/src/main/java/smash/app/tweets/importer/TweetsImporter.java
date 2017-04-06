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
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.opengis.feature.simple.SimpleFeature;
import smash.data.tweets.Tweet;
import smash.data.tweets.TweetsFeatureFactory;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.JobTimer;
import smash.utils.spark.FeatureRDDToGeoMesa;

import java.io.IOException;
import java.util.*;

/**
 * @author Yikai Gong
 */

public class TweetsImporter {

  // Stanford NLP properties
  private static Properties props = new Properties();
  private static List<String> keepPOS = new ArrayList<>();
  private static StanfordCoreNLP pipeline;

  // Initiate NLP on each spark worker in class loader
  static {
    props.setProperty("annotators",
      "tokenize, ssplit, pos, lemma, parse, sentiment");
    props.setProperty("tokenize.whitespace", "false");
    props.setProperty("ssplit.tokenPatternsToDiscard", "allDelete");
    pipeline = new StanfordCoreNLP(TweetsImporter.props);
    /**
     * These are the Part of Speech to retain for topic modelling
     *
     * @see https://catalog.ldc.upenn.edu/docs/LDC95T7/cl93.html
     * */
    keepPOS.addAll(Arrays.asList(new String[]{"FW", "JJ", "JJR", "JJS", "NN",
      "NNS", "NNP", "NNPS", "RB", "RP", "VB", "VBD", "VBG", "VBN", "VBP",
      "VBZ", "WRB"}));
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    TweetsImporterOptions options = new TweetsImporterOptions();
//    options.parse(args);
    TweetsImporter importer = new TweetsImporter();
    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

  private SparkConf sparkConf;

  public TweetsImporter(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
    sparkConf.setAppName(this.getClass().getSimpleName());
  }

  public TweetsImporter() {
    this(new SparkConf());
  }

  public void run(TweetsImporterOptions options) throws IOException {
    // Ensures Feature Type is saved in GeoMesa
    GeoMesaDataUtils.saveFeatureType(options, TweetsFeatureFactory.SFT);
    // Launch Spark Context
    try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
      JavaRDD<String> rawTweets = sc.textFile(options.inputFile, 100);
      JavaRDD<SimpleFeature> parsedTweets = rawTweets.flatMap(inLine -> {
        ArrayList<SimpleFeature> tweets = new ArrayList<>();
        try {
          Tweet tweet = Tweet.fromJSON(new JSONObject(inLine));
          tweet = processTweet(tweet);
          tweets.add(TweetsFeatureFactory.createFeature(tweet));
        } catch (Exception e) {
          System.err.println(e.getMessage());
        }
        return tweets.iterator();
      });
      Long nSaved = FeatureRDDToGeoMesa.saveToGeoMesa(options, parsedTweets, sc);
      System.out.format(">>> Ingested %,8d%n tweets", nSaved);
    }
  }

  protected static Tweet processTweet(Tweet tweet) throws Exception {
    Annotation document = new Annotation(tweet.getText());
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
    tweet.setSentiment((int) Sentiment.getSentiment(mainSentiment).getValue());
    tweet.getTokens().addAll(lemmas);
    return tweet;
  }
}
