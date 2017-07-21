package smash.app.tweets.cluster;

import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * @author Yikai Gong
 */

public abstract class SerializableFunction1<T1,R> extends AbstractFunction1<T1, R> implements Serializable {


}