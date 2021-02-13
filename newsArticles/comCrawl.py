from comcrawl import IndexClient
import pandas as pd

client = IndexClient(["2019-51"])
client.search("bbc.com/news/*")

# client.results = (pd.DataFrame(client.results)
#                   .sort_values(by="timestamp")
#                   .drop_duplicates("urlkey", keep="last")
#                   .to_dict("records"))

client.download()

client.results

# pd.DataFrame(client.results).to_csv("results.csv")