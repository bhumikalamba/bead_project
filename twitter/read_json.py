import json
import csv

with open('C:/Users/suren/Documents/bead_project/twitter/users_info_85082_v1.json') as f:
  data = json.load(f)

ids = []

for id in data['data']:
  ids.append(id[0])

missed_ids = []
missed_count = 0
excel_ids = []

with open('C:/Users/suren/Documents/bead_project/bq_20210320_users.csv', 'r') as file:
    reader = csv.reader(file)
    count  = 0
    for row in reader:
      if count  > 0:
        excel_ids.append(row[0])
        if row[0] not in ids:
          missed_count = missed_count + 1
          print(missed_count)
          missed_ids.append(row[0])
      count = count + 1

print(missed_ids)
print(len(missed_ids))
with open("missed_ids.txt", "w") as fp:
  json.dump(missed_ids, fp)
# for d in data:
#   print (d)
#
# ids = []
#
# for id in data['data']:
#   print(id[0])





