import feedparser

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts] 
             for i in range(wanted_parts) ]
  
feedURLs =['https://www.theguardian.com/world/rss', 'http://rss.nytimes.com/services/xml/rss/nyt/Europe.xml', 'http://www.independent.co.uk/news/rss', 'http://feeds.skynews.com/feeds/rss/world.xml', 'http://rss.nytimes.com/services/xml/rss/nyt/World.xml', 'https://www.cnet.com/rss/news/', 'http://rss.dw.com/rdf/rss-en-all']

lines = []

i = 0
for url in feedURLs:
  NewsFeed = feedparser.parse(url)
  print (url + ' Number of RSS posts :' + str(len(NewsFeed.entries)))
  for e in NewsFeed.entries:
    found = False
    for element in lines:
      if element['link'] == e.id:
        found = True
        print('Duplicate')
    if not found and len(e.summary) > 0:
      lines.append({'id': i, 'link':e.link, 'title':e.title, 'summary':e.summary, 'date': e.updated})
      i+=1
    

data = split_list(lines, 2)

import collections
import itertools
import operator
import multiprocessing
import string
import math

stopwords = ['ourselves', 'the', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each',  'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their',
             'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than']

# Source of SimpleMapReduce ---> https://pymotw.com/2/multiprocessing/mapreduce.html [Author: Doug Hellmann]


class SimpleMapReduce(object):

    def __init__(self, map_func, reduce_func, num_workers=None):
        """
        map_func

          Function to map inputs to intermediate data. Takes as
          argument one input value and returns a tuple with the key
          and a value to be reduced.

        reduce_func

          Function to reduce partitioned version of intermediate data
          to final output. Takes as argument a key as produced by
          map_func and a sequence of the values associated with that
          key.

        num_workers

          The number of workers to create in the pool. Defaults to the
          number of CPUs available on the current host.
        """
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)

    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs, chunksize=1):
        """Process the inputs through the map and reduce functions given.

        inputs
          An iterable containing the input data to be processed.

        chunksize=1
          The portion of the input data to hand to each worker.  This
          can be used to tune performance during the mapping phase.
        """
        map_responses = self.pool.map(
            self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values


def rss_to_words(id):
    print (multiprocessing.current_process().name +
           ' reading sub array with ID: ' + str(id))

    output = []
    # For each document in the list of documents for this process
    for line in data[id]:
        # Split the string into list of words (remove punctuation)
        words = [x.strip(string.punctuation) for x in line['summary'].split()]

        # For each word --> if it is a 'good' word --> add to output list (word, documentID, wordOccurance = 1)
        for word in words:
            word = word.lower()
            if word.isalpha() and word not in stopwords:
                output.append((word, (line['id'], 1)))

    return output


def list_to_dict(item):
    # Return a word + documents containing the word + the word occurance count for the documents
    word, docval = item

    groups = collections.defaultdict(int)

    for docid, value in docval:
        groups[docid] += value

    docArray = [{'occ': 0, 'DF': 0, 'IDF': IDF}] * len(lines)

    for docID, wordOccurance in groups.items():
        docArray[docID] = {'occ': wordOccurance,
                           'DF': 1 + math.log10(wordOccurance), 'IDF': IDF}
    return (word, docArray)


# From Lab Sheet 3
def calc_inner(v1, v2):  # receives two vectors of the same length as lists and returns their inner product
    ans = 0

    for i in range(len(v1)):
        ans += v1[i]*v2[i]

    return ans


def calc_length(v):  # receives a vector as a list and returns its length
    tmp = 0

    for x in v:
        tmp += x**2

    return math.sqrt(tmp)


def calc_cosine(v1, v2):  # receives two vectors of the same length as lists and returns their cosine similarity
    return calc_inner(v1, v2) / (calc_length(v1)*calc_length(v2))


if __name__ == '__main__':

    #Construct a Map Reduce and give it the according functions
    mapper = SimpleMapReduce(rss_to_words, list_to_dict)
    #Start the map - reduce by giving it the according pointers to part of the data
    word_counts = mapper([0, 1])

    # Convert output to dictionary
    wm = {}
    for word, docarray in word_counts:
        wm[word] = docarray

    print (str(len(wm.keys())) + " words in " +
           str(len(lines)) + " documents.")


    #Enter our query
    query = 'brexit deal'.split()

    #Will use only known words from our query
    vectorizedQueries = {}

    for word in query:
        word = word.lower()
        if word in wm.keys():
            if word not in vectorizedQueries.keys():
                vectorizedQueries[word] = {'count': 1, 'valForDocs': wm[word]}
            else:
                vectorizedQueries[word]['count'] += 1

    #To store the combined score for each document
    scores = [{'docID': 0, 'value': 0}] * len(lines)

    #For each term in our query
    for t in vectorizedQueries.keys():
        #For each document
        for i in range(len(lines)):
            #If this is the first term in the query --> add documentID and calculated TF-IDF for term
            if i != scores[i]['docID']:
                scores[i] = {'docID': i, 'value': wm[t]
                             [i]['DF'] * wm[t][i]['IDF']}
            #Else --> ADD the calculated TF-IDF for the term in the query
            else:
                scores[i]['value'] += wm[t][i]['DF'] * wm[t][i]['IDF']

    #Get the top 5 Highes scores
    sortedScores = sorted(scores, key=lambda x: x['value'], reverse=True)[:5]

    #Print output
    for score in sortedScores:
        print('Score: ', end=' ')
        print(score['value'])

        print('Title: ', end=' ')
        print(lines[score['docID']]['title'], end=' | UPDATED: ')
        print(lines[score['docID']]['date'])

        print('Summary: ', end=' ')
        print(lines[score['docID']]['summary'])

        print('Link: ', end=' ')
        print(lines[score['docID']]['link'])
        print('=======================')

    # PRINT word and list of documents containing the word
    # for word, count in word_counts:
    #    print()
    #    print (word, end=' = ')
    #    for docID, wordCountForDoc in count.items():
    #      print (str(docID) + ": " + str(wordCountForDoc), end=', ')

    #Last Update --> Cos Similarity Calculation

    #We want to convert our word - documents pairs to document - words paris
    vectorized = [[]] * len(lines)
    print(len(vectorized))
    for documentI in range(len(lines)):
        values = []
        for word in wm.keys():
            values.append(wm[word][documentI]['DF'] *
                          wm[word][documentI]['IDF'])
        vectorized[documentI] = values

    vQuery = []
    
    #Same thing for the query
    for word in wm.keys():
        if word in vectorizedQueries.keys():
            vQuery.append(
                (1 + math.log10(vectorizedQueries[word]['count'])) * vectorizedQueries[word]['valForDocs'][0]['IDF'])
        else:
            vQuery.append(0)  # wm[word][0]['IDF']

    print('\n\n\n\nCOS SCORES\n\n')
    cos_res = [{'docID': 0, 'value': 0}] * len(vectorized)
    for i in range(len(vectorized)):
        cos_res[i] = {'docID': i, 'value': calc_cosine(vQuery, vectorized[i])}

    sortedCosScores = sorted(
        cos_res, key=lambda x: x['value'], reverse=True)[:5]

    for score in sortedCosScores:
        print('Score: ', end=' ')
        print(score['value'])

        print('Title: ', end=' ')
        print(lines[score['docID']]['title'], end=' | UPDATED: ')
        print(lines[score['docID']]['date'])

        print('Summary: ', end=' ')
        print(lines[score['docID']]['summary'])

        print('Link: ', end=' ')
        print(lines[score['docID']]['link'])
        print('=======================')
