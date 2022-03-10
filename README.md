# Ukraine_tweets

![header](Imgs/no_war_header.jpeg)

# Project Motivation

Thursday, February 24, 2022 Russian army invades Ukraine and marks the beginning of a war that threatens Western countries, accompanied by a humanitarian refugee crisis. The purpose of this project is to obtain the Geolocation of Ukrainian Twitter Users. One of the limitations to using Twitter is that only about 1% of tweets are geotagged with the tweet's location, which can make much of this work very difficult.

### Assignment Description
Our dearly Stream Processing & Real-Time Analytics professor Raul Marín has proposed us a challenge. To deploy an end-to-end real-time solution following the next stages throughout the pipeline:<br>
  1- Select the **Data Source**: Twitter API.<br>
  2- **Ingestion**: Producer - Python app.<br>
  3- **Stream Storage**: Broker - Kafka.<br>
  4- **Processing**: Consumer - Spark Streaming.<br>
  5- **Serving**: Relational DB - MariaDB.<br>
  6- **Visualization**: Superset.<br>

### Who are we?
IE Students of the MSc Business Analytics & Big Data. Team Power Rangers:
  - Isobel Rae Impas
  - Jan P. Thoma
  - Camila Vasquez
  - Santiago Alfonso Galeano
  - Miguel Frutos

# Ukraine_tweets
### 1- Select the Data Source: Twitter API
Setting up Twitter. You need to create a "Twitter App". Follow the next steps:
- Visit [Projects & Apps](https://developer.twitter.com/en/portal/projects-and-apps) section in the Developer Portal
- Sign into your account. (if you dont have one you should create it).
- Click the “Create an app” button.
- Fill-in the form (at least required fields).
- Grab the details to setup the ingestion script later on.
- Once you’re in, click the “Create an app” button to start the process.
- Fill-in required fields.
- If all goes well, your Twitter App should be created and the API Key, API Secret Key will show up.
- The Access Token and Access Token Secret will be also needed.

### 2- Ingestion - Producer (Python app)
[Tweepy](http://www.tweepy.org/)  is a python wrapper for the Twitter API that allowed us to easily collect tweets in real-time.

```python



```
### 3- Stream Storage: Broker (Kafka)


### 4- Processing: Consumer (Spark Streaming)


### 5- Serving: Relational DB (MariaDB)


### 6- DataViz: Superset

# Insights


# Final Notes


# Bibliography
- Project by Raul Marin & Hupperich-Manuel https://github.com/raulmarinperez/bsdprof/tree/main/big_data/stream_processing/templates/twitter
- Project by Shawn Terryah https://github.com/shawn-terryah/Twitter_Geolocation
- Project by TuringTester https://github.com/TuringTester/Twitter-Data-Mining-Python
- Bounding Boxes by country https://gist.github.com/graydon/11198540
- 
