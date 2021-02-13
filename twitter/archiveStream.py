import internetarchive
search = internetarchive.search_items('collection:twitterstream')
for item in search:
    print(item['identifier'])

twitterStream = internetarchive.get_item("archiveteam-twitter-stream-2020-12")
file = twitterStream.get_file("twitter-stream-2020-12-02" + ".zip")

try:
    file.download()
except Exception as ex:
    print(ex)
print(twitterStream)
print(file)