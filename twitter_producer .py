"""
Authors - Team C:
- Isobel Rae Impas
- Jan P. Thoma
- Camila Vasquez
- Santiago Alfonso Galeano
- Miguel Frutos

Based on the code provided by Raul Marin & Hupperich-Manuel
https://github.com/raulmarinperez/bsdprof/tree/main/big_data/stream_processing/templates/twitter

Description: Scan the latest twitter feeds originating from a particular country, using Twitter’s Streaming API. The program creates a file which stores raw twitter streams with the name stream_<query given as argument>.json at the data path mentioned in the config.py file for testing purposes.

Required Packages: tweepy, argparse, time, string, json, prettytable

Usage: python3 twitter_producer.py -q 'hashtag' -c 'country'

Note: Fill up the variable in credentials.ini which contains data path and twitter app credentials.


"""
import configparser, argparse, logging, socket, tweepy, socket, sys

from confluent_kafka import Producer
from tweepy.streaming import Stream

# Auxiliary classes
#
class TwitterStreamListener(tweepy.Stream):

    _kafka_producer = None
    _topic = None
    
    def connect_to_kafka(self, broker, topic):
        """The Producer is configured using a dictionary. Added the topic inside the funtion"""
        conf = {'bootstrap.servers': broker,
                'client.id': socket.gethostname()}        
        self._kafka_producer = Producer(conf)
        self._topic = topic
  
    def on_data(self, data):
        """Add the logic you want to apply to the data. This method is called whenever new data arrives from live stream.
        We synchronously push this data to kafka queue (using produce and flush)"""
        if self._kafka_producer!=None:
            self._kafka_producer.produce(self._topic, value=data) #method enqueues message immediately for batching, compression and transmission to broker, no delivery notification events will be propagated until flush() is invoked.
            self._kafka_producer.flush() #Wait for all messages in the Producer queue to be delivered. This is a convenience method that calls poll() until len() is zero or the optional timeout elapses. flush() will block until the previously sent messages have been delivered (or errored), effectively making the producer synchronous.
            logging.debug(f"tweet: {data}")
        else:
            print(data)

    def on_error(self, status):
        """On error - if an error occurs, display the error / status"""
        #logging.error(status)
        print("Error received in kafka producer " + repr(status))
        return sys.exit(-1) ## Don't kill the stream. tells the program to quit,it stops from continuoing the execution

    def get_parser():
        """Get parser for command line arguments."""
        logging.basicConfig(level=logging.INFO) #Logging is a means of tracking events that happen when some software runs
        parser = argparse.ArgumentParser(description="Scanning Twitter Stream")
        parser.add_argument("credentials_file", 
                            help="path to the file with info to access the service")
        #parser.add_argument("filters", 
         #                   help="provide the filters matching the tweets you want to get")
        parser.add_argument("-c",
                            "--country",
                            dest="country",
                            help="Country/Location",
                            default='India')

        parser.add_argument("-b", "--broker",
                            help="server:port of the Kafka broker where messages will be published")
        parser.add_argument("-t", "--topic",
                            help="topic where messages will be published")  
        return parser    
   
# Body of the scripts       
#
    
if __name__ == '__main__':
    #Parse the argument given in the commandline
    logging.basicConfig(level=logging.INFO) #Logging is a means of tracking events that happen when some software runs
    parser = argparse.ArgumentParser(description="Scanning Twitter Stream")
    parser.add_argument("credentials_file", 
                         help="path to the file with info to access the service")
        #parser.add_argument("filters", 
         #                   help="provide the filters matching the tweets you want to get")
    parser.add_argument("-b", "--broker",
                            help="server:port of the Kafka broker where messages will be published")
    parser.add_argument("-t", "--topic",
                            help="topic where messages will be published")  
      #return parser   
    #parser = get_parser()
    args = parser.parse_args()   

  # Read credentials to connect to the Twitter Stream
  #
    credentials = configparser.ConfigParser()
    credentials.read(args.credentials_file)
        
    API_key = credentials['DEFAULT']['API_key']
    API_secret = credentials['DEFAULT']['API_secret']
    access_token = credentials['DEFAULT']['access_token']
    access_secret = credentials['DEFAULT']['access_secret']
    
  # Other alternative would be using OAuthHandler instead of the credentials.ini file  
    #from tweepy import OAuthHandler

    #make sure to keep your access keys secret
    #access_token = "(your own)"             
    #access_token_secret =  "(your own)"
    #api_key =  "(your own)"
    #api_secret =  "(your own)"

    #auth = OAuthHandler(api_key, api_secret)
    #auth.set_access_token(access_token, access_token_secret)    


  # Twitter connection and Kafka producer initialization
  #  
    twitter_conn = TwitterStreamListener(API_key, API_secret,
                                         access_token, access_secret)
    
  # Initialize the Kafka producer if broker and topic was specified
if args.broker != None and args.topic != None:
    twitter_conn.connect_to_kafka(args.broker, args.topic)
    # Start the filtering. Filter the Twitter stream based on the country passed as argument
    LOCATION = (22.0856083513, 44.3614785833, 40.0807890155, 52.3350745713)
    twitter_conn.filter(locations=LOCATION)
    twitter_conn.sample()

else:
    twitter_conn.filter(locations=LOCATION)
    twitter_conn.sample()
    