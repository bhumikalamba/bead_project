from pyspark.sql import SparkSession
import os
import datetime
import glob
import shutil
from google.cloud import storage


############ YOUR CONFIG ############

# dir for articles that were downloaded by commoncrawl.py
my_input_file_path = "/home/susurendhar/BEAD/newsArticles/news-please/cc_download_articles"
# dir to save clean batch of articles temporarily in vm
vm_file_path = '/home/susurendhar/BEAD/newsArticles/news-please/cc_store'
# GCS bucket to save data to
my_bucket = "bead-data"
# directory in GCS bucket to save data to
my_bucket_folder = "newsArticles"

# keep articles of certain languages
my_filter_language = ["en"]
# keep articles containing certain keywords in the main text, title or description
## the argument needs to be in a regular expression format
## use lowercase for the keywords.
## Spark uses Scala regex. So "(?i)" means case insensitive
my_filter_keywords = "(?i)bitcoin|btc|blockchain|cryptocurrenc(y|ies)|crypto|satoshi"

# delete batch json files after transferring saving temporarily to vm??
## indicate True only if you are sure!
delete_batch_json = False

# delete copy in vm after transferring to GCS bucket?
## indicate True only if you are sure!
delete_file_from_vm = True

############ END YOUR CONFIG #########
folders = [x[0] for x in os.walk(my_input_file_path)]

# Setup
#######

os.makedirs(vm_file_path, exist_ok=True)

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
        .load(local_download_dir_article + "/*.json")
        #.load(local_download_dir_article + "/*/*.json")
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


def upload_from_gce_to_gcs(local_path,bucket,gcs_path):
    assert os.path.isdir(local_path)
    for local_file in glob.glob(local_path + '/**'):
        if not os.path.isfile(local_file):
            upload_from_gce_to_gcs(local_file,bucket,gcs_path + "/" + os.path.basename(local_file))
        else:
            remote_path = os.path.join(gcs_path,local_file[1 + len(local_path):])
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(local_file)



def process_articles_cluster(my_input_file_path,vm_file_path,my_bucket,my_bucket_folder):

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


    print("saving DataFrame to " + vm_file_path + "/" + job_datetime + "...")
    ''' This option saves file as orc format
    articles_clean.write.format("orc") \
        .mode("overwrite") \
        .save(os.path.join(vm_file_path, job_datetime))
    '''
    ''' This option saves file as json format
    articles_clean.write.format("json") \
        .mode("overwrite") \
        .save(os.path.join(vm_file_path, job_datetime))
    '''
    ''' This option saves file as Parquet format'''
    articles_clean.write.format("parquet") \
        .mode("overwrite") \
        .save(os.path.join(vm_file_path, job_datetime))
    ''''''
    ''' This option saves file as CSV format. Note: Doesn't work. CSV does not support array data type.
    articles_clean.write.format("csv") \
        .option("header","true") \
        .option("sep",",") \
        .mode("overwrite") \
        .save(os.path.join(vm_file_path, job_datetime))
    '''
    print("done!")
    # Transfer Parquet to GCS bucket
    client = storage.Client()
    bucket = client.get_bucket(my_bucket)

    print('uploading file to GCS bucket...')
    upload_from_gce_to_gcs(vm_file_path, bucket, my_bucket_folder)
    print('done!')
# Run
#####
for fold in folders:
    if len(fold) != len(my_input_file_path):
        process_articles_cluster(fold,vm_file_path,my_bucket,my_bucket_folder)
        if delete_file_from_vm == True:
            print("deleting files from vm....")
            files = glob.glob(vm_file_path + '/*')
            for f in files:
                shutil.rmtree(f)
            print('done!')



# remove the batch of json files, proceed only if you are sure!
# if delete_batch_json == True:
#     print("deleting all json news files for this batch...")
#     files = glob.glob(fold + '/*')
#     for f in files:
#         shutil.rmtree(f)
#     print("done!")
# remove the files in GCE, proceed only if you are sure!








