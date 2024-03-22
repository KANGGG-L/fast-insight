from pyspark import SparkContext, SparkConf, AccumulatorParam
import sys
import math

class DictFreqAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}
    
    def addInPlace(self, dict1, dict2):
        for key, value in dict2.items():
            dict1[key] = dict1.get(key, 0) + value
        return dict1

def accumulateFreq(record):
    global termFreq
    termFreq += record[2]


class DictRecordAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}
    
    def addInPlace(self, dict1, dict2):
        for key, value in dict2.items():
            dict1[key] = value
        return dict1


def mapRecord(tokenizedRecord):
    # create inverted index for token
    global rIdRecordMap 
    id = tokenizedRecord[1][0]
    token = tokenizedRecord[1][2]

    rIdRecordMap += {id: token}
    

def constructRecord(line):
    '''
        Input: haedline->string
            e.g. 20191124,woman stabbed adelaide shopping centre

        Output: (Id->int, year->int, localTermFreq->dict of freq where key = term->string and value = frequency->int)
            e.g. (
                    0, => id of given headline 
                    2019, => year
                    {woman 1, stabbed: 1, adelaide: 1, shopping: 1, centre: 1}, => dict of freq
                )

    '''
    headline = line[0].strip().split(",")
    year = int(headline[0][:4])

    localTermsFreq = {}
    tmp = headline[1]
    if len(tmp) > 0:
        tmp = tmp.split(" ")
        for term in tmp:
            # calculate local term freq
            localTermsFreq[term] = localTermsFreq.get(term, 0) + 1

    return (line[1], year, localTermsFreq)

def calPrefixLength(recordLen, threshold):
    '''
        Input: recordLen->int, threshold->double

        Output: prefixLen->int
    '''
    return recordLen - math.ceil(recordLen * threshold) + 1


def prefixPartition(record, sortedTermFreq, tau):
    '''
        Input: haedline->string
            e.g. 20191124,woman stabbed adelaide shopping centre

        Output: (token->int, (Id->int, year->int, tokens->list of token))
            e.g. (
                    13, => token of wowam
                    (
                        0, => id of headline
                        2019, => year
                        [13, 22, 33, 43, 51], => list of tokens of terms in headline                                        
                    )
                )
    '''
    token = set()
    res = []

    if record[2] != {}:
        # convert terms to tokens
        for key, _ in record[2].items():
            token.add(sortedTermFreq.get(key))
        token = sorted(token)

        # generate prefix partitioned record
        prefixLength = calPrefixLength(len(token), tau)
        for tokenIndex in range(len(token)):
            if tokenIndex == prefixLength: break
            res.append((token[tokenIndex], (record[0], record[1], token)))

    return res

def constructIdPair(groupedRecord):
    '''
        Input: (year->int, partitionedRecordList->list)
            e.g. (
                    73916, => token of term 'random'
                    [
                        (
                            359570, => id of headline
                            2004 => year of headline
                        ),
                        (
                            159570, => id of headline
                            2002 => year of headline
                        )
                    ]
                )

        Output: res-> list of id pair
            e.g. [
                    (
                        (
                            159570, => id of headline 1 
                            359570 => id of headline 2
                        ),
                        1 => indication of such pair exist
                    )

                ]
    '''
    res = []

    partitionedRecordList = groupedRecord[1]
    partitionedRecordListLen = len(partitionedRecordList)

    for outerPartitionedRecordListIndex in range(partitionedRecordListLen):
        
        year1 = partitionedRecordList[outerPartitionedRecordListIndex][1]

        if outerPartitionedRecordListIndex != partitionedRecordListLen - 1:
            for innerPartitionedRecord in partitionedRecordList[outerPartitionedRecordListIndex + 1:]:

                year2 = innerPartitionedRecord[1]

                if year1 == year2:
                    continue
                else:
                    # construct id pairs for pairs with same token as prefix and headlines are not been published from the same year
                    
                    id1 = partitionedRecordList[outerPartitionedRecordListIndex][0]
                    id2 = innerPartitionedRecord[0]

                    if year1 < year2:
                        res.append(((id1, id2), 1))
                    else:
                        res.append(((id2, id1), 1))

    return res


def calSimilarity(token1, token2):
    '''
        Input: (token1->list of token, token2->list of token) 

        Output: similarity ->double 
    '''
    token1 = set(token1)
    token2 = set(token2)
    return len(token1.intersection(token2)) / len(token1.union(token2))


class Insight:

    def run(input_path, output_path, tau):
        
        conf = SparkConf().setAppName("Insight")
        sc = SparkContext(conf=conf)   
        file = sc.textFile(input_path)

        global termFreq,rIdRecordMap
        termFreq = sc.accumulator({}, DictFreqAccumulator())
        rIdRecordMap = sc.accumulator({}, DictRecordAccumulator())

        # assign headline id for each headline
        record = file.zipWithIndex().map(lambda line: constructRecord(line))

        # calculate frequency of each term from headline
        record.foreach(accumulateFreq)

        # use the value of dict to store the order of that key's frequency (i.e. integer 0 will be the token of term with smallest frequency)
        # then braodcast it for replacing actual terms
        sortedTermFreq = dict(sorted(termFreq.value.items(), key = lambda freq: (freq[1], freq[0])))
        for index, (key, _) in enumerate(sortedTermFreq.items()):
            sortedTermFreq[key] = index
        sortedTermFreq = sc.broadcast(sortedTermFreq)

        # convert terms in headline to tokens and generate record based on prefix 
        tokenizedRecord = record.flatMap(lambda record: prefixPartition(record, sortedTermFreq.value, tau))

        # create inverted index for record
        tokenizedRecord.foreach(mapRecord)
        recordSetMap = sc.broadcast(rIdRecordMap.value)
 
        # replace record by index of record
        # group records with same token in the prefix together
        # construct id pairs for record with same token in the prefix
        # group id pairs with same id together to prevent computing similarity duplicate times for same pair
        IdPair = tokenizedRecord.mapValues(lambda tokenizedRecord: (tokenizedRecord[0], tokenizedRecord[1])).groupByKey().mapValues(list).flatMap(lambda groupedRecord: constructIdPair(groupedRecord)).reduceByKey(lambda pair1, pair2: pair1)
        
        # calulate similarity for each similarityPair
        # filter out id pairs that dont meet the requirement
        # sort id pairs based on the key (i.e. pair of id)
        similarityPair = IdPair.map(lambda IdPair: (IdPair[0], calSimilarity(recordSetMap.value.get(IdPair[0][0]), recordSetMap.value.get(IdPair[0][1])))).filter(lambda similarityPair: similarityPair[1] >= tau).sortByKey()

        # convert RDD to expected format and output
        res = similarityPair.map(lambda similarityPair: "(" + str(similarityPair[0][0]) + "," + str(similarityPair[0][1]) + ")\t" + str(similarityPair[1]))
        res.saveAsTextFile(output_path)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Wrong inputs")
        sys.exit(-1)
    
    Insight.run(sys.argv[1], sys.argv[2], float(sys.argv[3]))
