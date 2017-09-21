package smash.utils.streamTasks.ml;

/**
 * Enumeration of sentiment
 */
public enum Sentiment {

  NEGATIVE, NEUTRAL, POSITIVE;

  public static Sentiment getSentiment(int sentimentClass) {

    if (sentimentClass == 2 || sentimentClass > 4 || sentimentClass < 0) {
      return Sentiment.NEUTRAL;
    } else if (sentimentClass > 2) {
      return Sentiment.POSITIVE;
    } else {
      return Sentiment.NEGATIVE;
    }
  }

  public int getValue() {

    if (this.equals(POSITIVE)) {
      return 1;
    }
    if (this.equals(NEGATIVE)) {
      return -1;
    }

    return 0;
  }
}
