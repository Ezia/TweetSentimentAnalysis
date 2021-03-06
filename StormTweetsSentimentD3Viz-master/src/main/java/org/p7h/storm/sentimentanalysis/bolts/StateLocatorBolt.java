package org.p7h.storm.sentimentanalysis.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Optional;
import org.p7h.storm.sentimentanalysis.utils.BingMapsLookup;
import org.p7h.storm.sentimentanalysis.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Gets the location of a tweet and forwards the state code and the tweet to the next Bolt.
 * There are three different objects within a tweet that we can use to determine it’s origin.
 * This class utilizes all the three of them and prioritizes in the following order [high to low]:
 *  1. The coordinates object
 *  2. The place object
 *  3. The user object
 *
 * @author - Prashanth Babu
 */
public final class StateLocatorBolt extends BaseRichBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateLocatorBolt.class);
  private static final long serialVersionUID = -8097813984907419942L;
  private OutputCollector _outputCollector;

  @Override
  public final void prepare(final Map map, final TopologyContext topologyContext,
                            final OutputCollector outputCollector) {
    this._outputCollector = outputCollector;
    
    final Properties properties = new Properties();
    try {
      properties.load(StateLocatorBolt.class.getClassLoader().getResourceAsStream(Constants.CONFIG_PROPERTIES_FILE));
    } catch (final IOException ioException) {
      //Should not occur. If it does, we can't continue. So exiting the program!
      LOGGER.error(ioException.getMessage(), ioException);
      System.exit(1);
    }
    /*
     * Bolt reads the Bing Maps API Value and stores the same to BING_MAPS_API_KEY_VALUE of Constants.java
     * so that the Bolt can use it.
     */
    Constants.BING_MAPS_API_KEY_VALUE = properties.getProperty(Constants.BING_MAPS_API_KEY);
  }

  @Override
  public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
    //Emit the state and also the complete tweet to the next Bolt.
    outputFieldsDeclarer.declare(new Fields("state", "tweet"));
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void execute(final Tuple input) {
    final Status status = (Status) input.getValueByField("tweet");
    final Optional<String> stateOptional = getStateFromTweet(status);
    
    if (stateOptional.isPresent()) {
      final String state = stateOptional.get();
      //Emit the state and also the complete tweet to the next Bolt.
      this._outputCollector.emit(new Values(state, status));
    }
  }

  /**
   * Tries to get the state of the tweet by checking first GeoLocation Object, then Place Object and finally User Object.
   *
   * @param status -- Status Object
   * @return State of the tweet
   */
  private final Optional<String> getStateFromTweet(final Status status) {
    String state = getStateFromTweetGeoLocation(status);
    state = getStateFromTweetPlaceObject(status, state);
    state = getStateFromTweetUserObject(status, state);

    if (state == null || !Constants.CONSOLIDATED_STATE_CODES.contains(state)) {
      return Optional.absent();
    }
    LOGGER.debug("State:{}", state);
    return Optional.of(state);
  }

  /**
   * Retrieves the state from User Object of the tweet.
   *
   * @param status -- Status Object
   * @param state -- Current state
   * @return State of tweet
   */
  private final String getStateFromTweetUserObject(final Status status, String state) {
    String stateFromUserObject = status.getUser().getLocation();
    
    if (state == null && null != stateFromUserObject && 1 < stateFromUserObject.length()) {
      String stateUser = stateFromUserObject.substring(stateFromUserObject.length() - 2).toUpperCase();
      LOGGER.debug("State from User:{}", stateFromUserObject);
      
      /*
       * Retry to get the State of the User if the last 2 chars are US for the User's Location object.
       * This is just a pro-active check and This assumes the format: NY, US
       */
      if ("US".equalsIgnoreCase(stateUser) && 5 < stateFromUserObject.length()) {
        stateUser = stateFromUserObject.substring(stateFromUserObject.length() - 6, stateFromUserObject.length() - 4);
        LOGGER.debug("State from User again:{}", stateFromUserObject);
      }
      state = (2 == stateUser.length()) ? stateUser.toUpperCase() : null;
    }
    return state;
  }

  /**
   * Retrieves the state from Place Object of the Tweet.
   *
   * @param status -- Status Object
   * @param state -- Current state
   * @return State of tweet
   */
  private final String getStateFromTweetPlaceObject(final Status status, String state) {
    final Place place = status.getPlace();
    
    if (state == null && place != null) {
      final String placeName = place.getFullName();
      
      if (placeName != null && 2 < placeName.length()) {
        final String stateFromPlaceObject = placeName.substring(placeName.length() - 2);
        LOGGER.debug("State from Place:{}", stateFromPlaceObject);
        state = (2 == stateFromPlaceObject.length()) ? stateFromPlaceObject.toUpperCase() : null;
      }
    }
    return state;
  }

  /**
   * Retrieves the State from GeoLocation Object of the Tweet.
   * This is considered as the primary and correct value for the state of the tweet.
   * Can also try: http://www.fcc.gov/developers/census-block-conversions-api
   *
   * @param status -- Status Object
   * @return State of tweet
   */
  private final String getStateFromTweetGeoLocation(final Status status) {
    String state = null;
    final double latitude;
    final double longitude;
    final GeoLocation geoLocation = status.getGeoLocation();
    
    if (geoLocation != null) {
      latitude = geoLocation.getLatitude();
      longitude = geoLocation.getLongitude();
      
      final Optional<String> stateGeoOptional = BingMapsLookup.reverseGeocodeFromLatLong(latitude, longitude);
      if (stateGeoOptional.isPresent()) {
        final String stateFromGeoLocation = stateGeoOptional.get();
        state = (2 == stateFromGeoLocation.length()) ? stateFromGeoLocation.toUpperCase() : null;
      }
    }
    return state;
  }
}