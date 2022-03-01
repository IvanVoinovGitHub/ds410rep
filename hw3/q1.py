#Question 1

#For each country, how much money was earned from it.
#output: key value pair (country, amount)
#last value of each line is country
#have to correct for spaces (examples: United Kingdom)

#mapper should ignored first line (characteristics)

#amount = unit price x quantity (4 x 6)

#from mrjob.job import MRJob   # MRJob version
from mockmr import MockMR      #MockMR version
import hashlib
import csv

#class WordCount(MRJob):  #MRJob version
class WCStats3(MockMR):  #MockMR version
    def mapper(self, key, line):
        
        parts = list(csv.reader([line]))[0]
        
        string_wout_spaces = parts.strip()
        
        countries = string_wout_spaces[-1]
        unit_prices = string_wout_spaces[-3]
        quantities = string_wout_spaces[-5]
        
        count = 0
        for w in countries:
            yield (w, unit_prices[count] * quantities[count])
            count = count+1
            
     
    def reducer(self, key, values):
        yield (key, sum(values))
    def partition(self, key, num_reducers):
        """ This is the default partitioner that you get if you do not implement it
        if you uncomment this function, you may want to import hashlib at the top 
of the file"""
        if key[0] == "_":
            return (int(hashlib.sha1(bytes(str(key),'utf-8')).hexdigest(), 16) % 
(num_reducers - 1)  ) + 1
        else:
            return 0
if __name__ == '__main__':
    #WordCount.run()   # MRJob version
    WCStats3.run(trace=True) #MockMR version
