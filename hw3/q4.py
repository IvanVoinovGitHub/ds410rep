#from mrjob.job import MRJob   # MRJob version
from mockmr import MockMR      #MockMR version
import hashlib
import csv



#10 reducers were estimated to be needed based off of the size of the dataset
NUM_REDUCERS = 10


#class WordCount(MRJob):  #MRJob version
class q4(MockMR):  #MockMR version
    global_total = 0

    def mapper_init(self):
        self.total = 0
        self.total_pamount = 0
    
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
            self.total += 1
            self.total_pamount += unit_prices[count] * quantities[count]
            
    def mapper_final(self):
        for i in range(NUM_REDUCERS):
            yield f"Total_{i}", self.total
        # this loop basically does the following:
        #yield "Total_0", self.total # we want this to go to reducer 0
        #yield "Total_1", self.total # we want this to go to reducer 1
        #yield "Total_2", self.total # we want this to go to reducer 2

    def reducer_init(self):
        self.total_amount = 0

    def reducer(self, key, values):
        # We want every reducer to know the total number of words
        # We want the reducers to know this *before* processing key value pairs       
        yield (key, sum(values) / self.total_pamount)

    def reducer_final(self):
        pass
        
        
    def partition(self, key, num_reducers):
        if key.startswith("Total_"):
            (junk, num) = key.split("_", 1) #junk will be "Total" and num will be the number
            return int(num)
        # this does the following
        #if key == "Total_0":
        #    return 0
        #if key == "Total_1":
        #    return 1
        #if key == "Total_2":
        #    return 2
        return int(hashlib.sha1(bytes(str(key),'utf-8')).hexdigest(), 16) % num_reducers

    def compare(self, a, b):
        """ This is the default sorter that you get if you do not implement it.
            returns -1 if a<b, 0 if a=b, 1 if a>b """
        #return (a > b) - (b > a)
        # our goal is that keys like Total_0 appear before all other keys so that the
        # reducer would see it first.
        if a == b:
            return 0
        if a.startswith("Total_") and b.startswith("Total_"): # just to be safe
            return (a > b) - (b > a) # default behavior when comparing Total_something in alphabetical order
        if b.startswith("Total_"): # if we are here we know a doesn't start with Total_
            return 1
        if a.startswith("Total_"): # if we are here we know b doesn't start with Total_
            return -1
        return (a > b) - (b > a) # otherwise do what would normally happen


if __name__ == '__main__':
    #WordCount.run()   # MRJob version
    q4.run(trace=True) #MockMR version
