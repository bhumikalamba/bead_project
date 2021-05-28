# Identification of Influential Online Profiles and Authorities for Short-Term Cryptocurrency Trading
Code to accompany the following paper: Identification of Influential Online Profiles and Authorities for Short-Term Cryptocurrency Trading

## Scripts to reproduce results

### 1) Setup
- Clone this repository
- To install dependencies:
	- run ```pip install -r requirements.txt```
	- If you add any libraries thru pip install type ```pip freeze > requirements.txt```
- To run PySpark in PyCharm IDE (for Windows):
	- add the following 3 paths to the project's Content Root in PyCharm IDE: 
		- PyCharm IDE > Settings > Project Structure > Add Content Root
		- C:\spark-2.4.7-bin-hadoop2.7\python\
		- C:\spark-2.4.7-bin-hadoop2.7\python\lib\py4j-0.10.7-src.zip
		- C:\spark-2.4.7-bin-hadoop2.7\python\lib\pyspark.zip

### 2) Data ingestion
**Bitcoin price data**

- Batch ingestion using bitfinex.instant.py
- Command to trigger script `python bitfinex_instant.py --date 2021-04-05`

**Twitter data**

- Twitter streaming using Flume `Twitter.conf`. This Ingests data into Google Cloud Storage(GCS)
- Twitter Historical data to fetch past 7 days data. Associated scripts *beginTwitterIngestion.py* and *twitterHistorical.py*
	`python beginTwitterIngestion.py`
  
- Batch processing to get data from GCS, cleanse and store in Google BigQuery.
  `python tweets-gcs-pyspark.py`
  
- Batch Processing to get twitter followers ID and ingest into neo4j database. Associated scripts *IngestFollowers-neo4j.py* & *model.py*
  `python IngestFollowers-neo4j.py`


**Online news data**
- Clone the [news-please repository](https://github.com/fhamborg/news-please) to ./newsArticles/. Our highest gratitude and appreciation to the news-please team for making it a breeze to extract data from commoncrawl.org. A million thanks to the [Common Crawl team](https://commoncrawl.org/about/team/) as well for crawling the web.
- Check requirements:
	- Python 3.5+. To check, run: `python --version`
	- Install [the awscli tool version 2](https://github.com/fhamborg/news-please). To check, run: `aws --version`
	- Check python libraries in news-please/requirements.txt. To check, run: `pip list`
- Configure parameters in the "YOUR CONFIG" section of news-please/newsplease/examples/commoncrawl.py
	- List of domain names 
	```
	my_filter_valid_hosts = ['forbes.com', 'cnn.com', 'seekingalpha.com', 'thestreet.com', 'marketwatch.com', 'coindesk.com', 'cointelegraph.com', 'todayonchain.com','newsbtc.com', 'cryptoslate.com']
	```
	- (optional) Number of cores to use (check your computer configuration)
	```
	my_number_of_extraction_processes = 3
	```
	- Date of the first warc file to download from
	```
	my_warc_files_start_date = datetime.datetime(2020, 1, 1) # 1st January 2020
	```
- Change directory to ./news-please in terminal.
- Run `python3 -m newsplease.examples.commoncrawl`
- We further filter the articles by only those that contain keywords that are related to Bitcoin. Adjust the parameters in the "YOUR CONFIG" section of newsArticles/parse_kw_language.py
- Dump this batch of articles to the database. Run: `python3 parse_kw_language.py`
- This is a batch process. Repeat the steps when there are new warc files by Common Crawl.

### 3) Data Processing

**Who To Follow - WTF**

- Batch Process on PySpark to filter data, get intermediary features (# of Tweets/Articles Published, # of Tweets/Articles Published in Golden Window, # of Golden Window Hits) and output the final WTF Score for Twitter users and News Sources.
Associated Script *getWTF.py* runs on local machine & *getWTF_tweetsBQ.py* with Google BigQuery connector that could be submitted as a Spark job on Google Dataproc.

**Apply GraphAlgorithms using PySpark**

- Batch Processing to apply Graph algorithms and save the data into BigQuery. Associated Script *neo4j-pyspark-conn.py* & *closeness_centrality.py*
  `python neo4j-pyspark-conn.py`
  
- Interim scripts to download data from neo4j and save it in JSON for analysis using Bloom.
  *Interim-script-neo4j.py*
  
