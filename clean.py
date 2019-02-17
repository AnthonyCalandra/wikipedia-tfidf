import re
from time import sleep

new_csv = open("wikipedia_utf8_filtered_20pageviews_new.csv", "a+")
old_csv = open("wikipedia_utf8_filtered_20pageviews.csv", "r")
for line in old_csv:
    matches = re.match(r'wikipedia\-(\d+),\"\s*(.+)\s*\"\s*', line)
    if matches:
        group = matches.groups()
        new_csv.write("%s\t%s\n" % (group))
old_csv.close()
new_csv.close()
