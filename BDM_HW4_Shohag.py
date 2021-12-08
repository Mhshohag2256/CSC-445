# Mehedi H Shohag

from pyspark import SparkContext
import datetime
import csv
import functools
import json
import numpy as np
import sys


def main(sc):
    '''
    Transfer our code from the notebook here, however, remember to replace
    the file paths with the ones provided in the problem description.
    '''
    rddPlaces = sc.textFile('/data/share/bdm/core-places-nyc.csv')
    rddPattern = sc.textFile('/data/share/bdm/weekly-patterns-nyc-2019-2020/*')
    OUTPUT_PREFIX = sys.argv[1]
    CAT_CODES = {'445210', '445110', '722410', '452311', '722513', '445120', '446110', '445299',
                 '722515', '311811', '722511', '445230', '446191', '445291', '445220', '452210', '445292'}
    CAT_GROUP = {'452210': 0, '452311': 0, '445120': 1, '722410': 2, '722511': 3, '722513': 4, '446110': 5, '446191': 5,
                 '722515': 6, '311811': 6, '445210': 7, '445299': 7, '445230': 7, '445291': 7, '445220': 7, '445292': 7, '445110': 8}

    print('CAT_CODES', CAT_CODES)
    print('CAT_GROUP', CAT_GROUP)
    ##########################
    # 0: placekey
    # 9: naics_code

    def filterPOIs(_, lines):
        for line in lines:
            line = line.split(',')
            if line[9] in CAT_CODES:
                yield (line[0], CAT_GROUP[line[9]])

    rddD = rddPlaces.mapPartitionsWithIndex(filterPOIs) \
        .cache()

    rddD.take(5)

    storeGroup = dict(rddD.collect())
    print(storeGroup['23g-222@627-wc8-7h5'])  # for sanity check, should be 6

    groupCount = rddD \
        .map(lambda x: (x[1], x[0])).groupByKey().mapValues(list).sortByKey(ascending=True).map(lambda x: len(x[1])) \
        .collect()
    groupCount

    #########################
    #  0: placekey
    # 12: date_range_start
    # 14: raw_visit_counts
    # 16: visits_by_day
    def extractVisits(storeGroup, _, lines):
        lines = csv.reader(lines)
        keys = list(storeGroup.keys())
        for line in lines:
            if line[0] in keys:
                data = []
                date_start = line[12][:10]
                date_temp = datetime.datetime.strptime(date_start, '%Y-%m-%d')
                visits_by_day = json.loads(line[16])
                for i in range(len(visits_by_day)):
                    date_temp += datetime.timedelta(days=1)
                    yield ((storeGroup[line[0]], date_temp.strftime("%Y-%m-%d")), visits_by_day[i])

    rddF = rddPattern \
        .mapPartitionsWithIndex(functools.partial(extractVisits, storeGroup))

    rddF.take(5)

    #########################
    #  0: placekey
    # 12: date_range_start
    # 14: raw_visit_counts
    # 16: visits_by_day
    def extractVisits(storeGroup, _, lines):
        current_date = datetime.datetime.strptime('2019-01-01', '%Y-%m-%d')
        lines = csv.reader(lines)
        keys = list(storeGroup.keys())
        for line in lines:
            if line[0] in keys:
                data = []
                date_start = line[12][:10]
                date_temp = datetime.datetime.strptime(date_start, '%Y-%m-%d')
                visits_by_day = json.loads(line[16])
                for i in range(len(visits_by_day)):
                    date_temp += datetime.timedelta(days=1)
                    days = date_temp - current_date
                    yield ((storeGroup[line[0]], days.days), visits_by_day[i])

    rddG = rddPattern \
        .mapPartitionsWithIndex(functools.partial(extractVisits, storeGroup))

    rddG.take(5)

    # Remember to use groupCount to know how long the visits list should be
    def computeStats(groupCount, _, records):
        data = []
        result = []
        for record in records:
            if record[0] in data:
                index = data.index(record[0])
                result[index].append(list(record[1]))
            else:
                data.append(record[0])
                result.append(list(record[1]))
        for i in range(len(result)):
            haha = result[i]
            if len(haha) % 2 == 1:
                median = haha[int(len(haha)/2)]
            else:
                median = (haha[0] + haha[-1]) / 2
            yield (data[i], (median, min(haha), max(haha)))
            # yield list(result[i])

    rddH = rddG.groupByKey() \
        .mapPartitionsWithIndex(functools.partial(computeStats, groupCount))

    rddH.take(5)
    # rddH.count()
    # print(rddG.count())
    # rddG.count()

    rddI = rddG.groupByKey() \
        .map(lambda x: (x[0][0], str((datetime.datetime.strptime('2019-01-01', '%Y-%m-%d') + datetime.timedelta(days=x[0][1])).year) + ',' + str((datetime.datetime.strptime('2019-01-01', '%Y-%m-%d') + datetime.timedelta(days=x[0][1])).strftime("%Y-%m-%d")).replace('2019', '2020') + ',' + str(list(x[1])[int(len(list(x[1]))/2)]) + ',' + str(min(list(x[1]))) + ',' + str(max(list(x[1])))))

    rddI.take(5)
    rddJ = rddI.sortBy(lambda x: x[1][:15])
    header = sc.parallelize([(-1, 'year,date,median,low,high')]).coalesce(1)
    rddJ = (header + rddJ).coalesce(10).cache()
    rddJ.take(5)
    # Remove the output folder if it's already there
    !rm - rf / content/output/*

    OUTPUT_PREFIX = '/content/output'
    filename = 'big_box_grocers'
    rddJ.filter(lambda x: x[0] == 0 or x[0] == -1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename}')

    !ls - 1 / content/output/big_box_grocers/part-*
    !cat / content/output/big_box_grocers/part-* | head
    # Remove the output folder if it's already there
    !rm - rf / content/output/*

    OUTPUT_PREFIX = '/content/output'

    filename = 'big_box_grocers'
    rddJ.filter(lambda x: x[0] == 0 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename}')

    filename1 = 'convenience_stores'
    rddJ.filter(lambda x: x[0] == 1 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename1}')

    filename2 = 'drinking_places'
    rddJ.filter(lambda x: x[0] == 2 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename2}')

    filename3 = 'full_service_restaurants'
    rddJ.filter(lambda x: x[0] == 3 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename3}')

    filename4 = 'limited_service_restaurants'
    rddJ.filter(lambda x: x[0] == 4 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename4}')

    filename5 = 'pharmacies_and_drug_stores'
    rddJ.filter(lambda x: x[0] == 5 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename5}')

    filename6 = 'snack_and_retail_bakeries'
    rddJ.filter(lambda x: x[0] == 6 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename6}')

    filename7 = 'specialty_food_stores'
    rddJ.filter(lambda x: x[0] == 7 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename7}')

    filename8 = 'supermarkets_except_convenience_stores'
    rddJ.filter(lambda x: x[0] == 8 or x[0] == -1).coalesce(1).values() \
        .saveAsTextFile(f'{OUTPUT_PREFIX}/{filename8}')

    !ls - 1 / content/output


if __name__ == '__main__':
    sc = SparkContext()
    main(sc)
