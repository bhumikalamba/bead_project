from pyspark.sql import SparkSession
import os
import datetime
import glob
import shutil

############ YOUR CONFIG ############

# dir for articles that were downloaded by commoncrawl.py
my_input_file_path = "/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/news-please/cc_download_articles"
# dir to save clean batch of articles in data store
my_output_file_path = "/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/news-please/cc_store"
# keep articles of certain languages
my_filter_language = ["en"]
# keep articles containing certain keywords in the main text, title or description
## the argument needs to be in a regular expression format
## use lowercase for the keywords.
## Spark uses Scala regex. So "(?i)" means case insensitive
my_filter_keywords = "(?i)bitcoin|btc|blockchain|cryptocurrenc(y|ies)|crypto|satoshi"
# delete batch json files after transferring cleaned data to store?
## indicate True only if you are sure!
delete_batch_json = False

############ END YOUR CONFIG #########


# Setup
#######

os.makedirs(my_output_file_path, exist_ok=True)

spark = SparkSession \
    .builder \
    .appName("parse keywords and language") \
    .getOrCreate()

sc = spark.sparkContext


# Functions
###########

def read_in_articles(local_download_dir_article):
    '''
    Read in all articles downloaded by commoncrawl.py
    '''
    articles_df = spark.read.format("json") \
        .option("multiline", "true") \
        .load(local_download_dir_article + "/*/*.json")
    return articles_df

def remove_duplicates(articles_df):
    '''
    Commoncrawl does not guarantee that they did not crawl and save a website more than once.
    We remove duplicate news articles here.
    '''
    print("remove duplicate articles.")
    return articles_df.dropDuplicates(["authors", "date_publish", "title", "description", "source_domain"])

def count_articles(articles_df):
    '''
    Gets the current number of articles in the DataFrame.
    '''
    print("Currently, total number of articles: %i" % articles_df.count())

def parse_language(filter_language):
    '''
    keep only articles from selected languages
    '''
    print("keep language(s): %s" % filter_language)
    return lambda articles_df: (
        articles_df.filter(articles_df["language"].isin(filter_language))
    )

def parse_keywords(filter_keywords):
    '''
    keep only articles containing keywords
    '''
    print("keep articles with keywords: \"%s\"" % filter_keywords)
    return lambda articles_df: (
        articles_df.filter(articles_df["maintext"].rlike(filter_keywords) |
                           articles_df["title"].rlike(filter_keywords) |
                           articles_df["description"].rlike(filter_keywords))
    )

def keep_columns(articles_df):
    print("keep just some columns.")
    return articles_df.select("authors",
                              "date_download",
                              "date_publish",
                              "description",
                              "filename",
                              "language",
                              "maintext",
                              "source_domain",
                              "title",
                              "url")


# Run
#####

articles = read_in_articles(my_input_file_path)
count_articles(articles)

articles_clean = (articles
                .transform(remove_duplicates)
                .transform(parse_language(my_filter_language))
                .transform(parse_keywords(my_filter_keywords))
                .transform(keep_columns)
                )

count_articles(articles_clean)
articles_clean.describe().show()
articles_clean.show(n=5,truncate=True)


# save DataFrame as orc format in data store
job_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

print("saving DataFrame to " + my_output_file_path + "/" + job_datetime + "...")
articles_clean.write.format("orc") \
    .mode("overwrite") \
    .save(os.path.join(my_output_file_path, job_datetime))
print("done!")

# remove the batch of json files, proceed only if you are sure!
if delete_batch_json == True:
    print("deleting all json news files for this batch...")
    files = glob.glob(my_input_file_path + '/*')
    for f in files:
        shutil.rmtree(f)
    print("done!")
