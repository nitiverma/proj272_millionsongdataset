"""
    A map-reduce that calculates the density for each
    of a set of tracks.  The track density is the average
    number of segments per segment for a track.
"""

from mrjob.job import MRJob
import track
from nltk import stem
import re

# if YIELD_ALL is true, we yield all densities, otherwise,
# we yield just the extremes

YIELD_ALL = True

def meanAndLength(val):
    sum = 0
    num = 0
    for value in val:
        sum += value
        num += 1
    return (sum / num, num)

class MRDensity(MRJob):
    """ A  map-reduce job that calculates the density """

    def __init__(self, args):
        super(MRDensity, self).__init__(args)
        self.f = open('results_year.csv', 'w')
        self.lancaster = stem.lancaster.LancasterStemmer()
        self.pattern = re.compile(r"[^a-z]*([a-z]+).*")

    def mapper(self, _, line):
        """ The mapper loads a track and yields its density """
        #t = track.load_track(line)
        print line
        (title, album, artist_name, year, duration, segments, tempo) = line.split(',')
        if tempo > 0:
            density = int(segments) / float(duration)
        yield (int(year) / 10 * 10), density

    def reducer(self, key, val):
        (meanVal, num) = meanAndLength(val)
        self.f.write(str(key) + ', ' + str(meanVal) + ', ' + str(num) + '\n')
        yield (key, meanVal)

if __name__ == '__main__':
    MRDensity.run()
