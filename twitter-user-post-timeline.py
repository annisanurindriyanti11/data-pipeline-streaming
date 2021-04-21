import tweepy
from tweepy import OAuthHandler
import json

def load_api():
    ''' Function that loads the twitter API after authorizing the user. '''

    consumer_key = 'GJQ0sqAftC0ZjptqgMdYqIgQR'
    consumer_secret = 'UD9xdh1LnGB4jIvUdNhlTzbkWqmC5BZ4lc4qlMVTYQPdkONPFv'
    access_token = '1082187503908442112-oiTa0TqOb6E00XpJNBL7yDaKK7r6ez'
    access_secret = 'RuOWAQ82fsg7FoW3xdg7q3wWOt9BNKHZZXWK1GQWlE07S'
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # load the twitter API via tweepy
    return tweepy.API(auth)

api = load_api()
user = 'spuryyc'
tweets = api.user_timeline(id=user, count=1000)
print('Found %d tweets' % len(tweets))

# You now have a list of tweet objects with various attributes
# check out the structure here: http://tkang.blogspot.ca/2011/01/tweepy-twitter-api-status-object.html

# For example we can get the text of each tweet
tweets_text = [t.text for t in tweets]
filename = 'tweets-'+user+'.json'
json.dump(tweets_text, open(filename, 'w'))
print('Saved to file:', filename)

# Can load file like this
#tweets_text = json.load(open(filename, 'r'))
