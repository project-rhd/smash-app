package smash.utils.streamTasks.ml.sentiment;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.slf4j.Logger;
import smash.utils.streamTasks.AbstractTask;

import java.util.*;

/**
 * @author Yikai Gong
 */

public class SentimentAnalysisTask<T extends String, U extends Map.Entry<Integer, List<String>>> extends AbstractTask<T, U> {
  private static StanfordCoreNLP pipeline;
  private static final ThreadLocal<SentimentAnalysisTask<String, Map.Entry<Integer, List<String>>>> t =
    ThreadLocal.withInitial(SentimentAnalysisTask::new);

  @SuppressWarnings("unchecked")
  public static SentimentAnalysisTask<String, Map.Entry<Integer, List<String>>> getThreadSingleton(Logger l_, Properties p_) {
    SentimentAnalysisTask singleton = t.get();
    singleton.setup(l_, p_);
    return singleton;
  }

  // Stanford NLP properties
  private Properties props = new Properties();
  private List<String> keepPOS = new ArrayList<>();

  private final Object locker = new Object();
  private boolean doneSetup = false;

  @Override
  public void setup(Logger l_, Properties p_) {
    super.setup(l_, p_);
    if (!doneSetup) {
      props.setProperty("annotators",
        "tokenize, ssplit, pos, lemma, parse, sentiment");
      props.setProperty("tokenize.whitespace", "false");
      props.setProperty("tokenize.options", "untokenizable=noneKeep");
      props.setProperty("ssplit.tokenPatternsToDiscard", "allDelete");
      keepPOS.addAll(Arrays.asList("FW", "JJ", "JJR", "JJS", "NN",
        "NNS", "NNP", "NNPS", "RB", "RP", "VB", "VBD", "VBG", "VBN", "VBP",
        "VBZ", "WRB"));
      if (pipeline == null) {
        SentimentAnalysisTask.pipeline = new StanfordCoreNLP(props);
      }
    }
    doneSetup = true;
  }

  public SentimentAnalysisTask() {
  }

  public SentimentAnalysisTask(Logger l_, Properties p_) {
    this();
    setup(l_, p_);
  }

  @Override
  protected Float doTaskLogic(Map<String, T> map) {
    if (!doneSetup || pipeline == null) {
      l.warn("Task:  " + this.getClass().getSimpleName() + " has not been fully setup");
      return 0F;
    }
    for (T v : map.values()) {
      if(v == null){
        l.warn("Input txt is null");
        continue;
      }
      List<String> tokens = new ArrayList<>();
      Integer score = sentimentAnalyze(v, tokens);
      Map<Integer, List<String>> result = new HashMap<>();
      result.put(score, tokens);
      setLastResult((U)result.entrySet().iterator().next());
    }
    return null;
  }

  @Override
  protected U setLastResult(U r) {
    return super.setLastResult(r);
  }

  private Integer sentimentAnalyze(String txt, List<String> tokens) {
    Annotation document = new Annotation(txt);
    pipeline.annotate(document);
    List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
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
        if (keepPOS.contains(pos)) {
          lemma = token.lemma().toLowerCase();
          if (!lemmas.contains(lemma)) {
            lemmas.add(lemma);
          }
        }
      }
    }

    Integer score = Sentiment.getSentiment(mainSentiment).getValue();
    if (tokens == null)
      tokens = new ArrayList<>();
    tokens.addAll(lemmas);

    return score;
  }
}
