# get and clean up for course the buzzfeed data on
# https://www.buzzfeednews.com/article/craigsilverman/facebook-fake-news-hits-2018
# https://github.com/BuzzFeedNews/2018-12-fake-news-top-50
# 'contains information about the top fake news articles of 2018 (by Facebook engagement)
# published by our 2018 list of fake news sites.'

# from github in case it disappears
#title: The title of the article.
#url: The URL of the article.
#fb_engagement: The number of Facebook engagements the article received. (See note below.)
#published_date: The date the article was published.
#category: The article's main theme, as categorized by BuzzFeed News. Category may be Crime, Politics, Medical, Music, Sports, Business, or uncategorized.
#source: The data source, if other than BuzzSumo. (See note below.)

import pandas as pd

# initial
# d = pd.read_csv('https://raw.githubusercontent.com/BuzzFeedNews/2018-12-fake-news-top-50/master/data/top_2018.csv')
# d.to_csv('./raw/fakebook_buzzfeed_2018.csv', index = False)

d = pd.read_csv('./raw/fakebook_buzzfeed_2018.csv')

# remove commas from dollar amounts !!!!

d = d.assign(fb_engagement = pd.to_numeric(d.fb_engagement.str.replace(',', '')))

d.to_csv('../stor155_sp21/data/fakebook_buzzfeed_2018.csv', index = False)
d.to_excel('../stor155_sp21/data/fakebook_buzzfeed_2018.xlsx', index = False)
