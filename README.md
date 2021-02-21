# Identification of Influential Online Profiles and Authorities for Short-Term Cryptocurrency Trading
Code to accompany the following paper: Identification of Influential Online Profiles and Authorities for Short-Term Cryptocurrency Trading

## Scripts to reproduce results

### 1) Setup
- Clone this repository
- To install dependencies:
	- run ```pip install -r requirements.txt```
	- If you add any libraries thru pip install type ```pip freeze > requirements.txt```

### 2) Data ingestion
**Bitcoin price data**

**Twitter data**

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
