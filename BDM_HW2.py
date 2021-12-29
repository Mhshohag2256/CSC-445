################################
###      Mehedi Shohag       ###
################################
import csv
import mapreduce as mr
from mrjob.job import MRJob
from mrjob.step import MRStep


################################
### YOUR WORK SHOULD BE HERE ###
################################
class MRFindReciprocal(MRJob):
    
    hash_table = {}
    def mapper1(self, _, row):
        data = row.strip().split(',')
        email_from = data[1]
        email_to = data[2]
        email_from_list = email_from.split(',')
        for i in email_from_list:
            if "@enron.com" in i:
                email_from = i.split('@')[0]
                ffirst_name = email_from.split('.')[0]
                flast_name = email_from.split('.')[-1]
                email_from = ffirst_name.title() + " " + flast_name.title()
                To_list = email_to.split(';')
                for j in To_list:
                    if "@enron.com" in j:
                        email_to = j.split('@')[0]
                        tfirst_name = email_to.split('.')[0]
                        tlast_name = email_to.split('.')[-1]
                        email_to = tfirst_name.title() + " " + tlast_name.title()
                        yield ([email_from, email_to])
    
    def reducer1(self, email_from, email_to):
        yield (email_from, set(email_to))

    def mapper2(self, email_from, email_to):
        self.hash_table[email_from] = email_to
        for i in email_to:
            yield (i, email_from)

    def reducer2(self, rcvr, email_from):
        if rcvr in self.hash_table:
            email = (set(email_from), self.hash_table[rcvr])
            yield (rcvr, email)

    def mapper3(self, rcvr, email):
        sender, reciver = email
        for i in sender:
            if i in reciver:
                couple = (rcvr, i)
                first, second = sorted(couple)
                yield ('reciprocal', first + ':' + second)

    def reducer3(self, _, name):
        name = sorted(set(name))
        for i in name:
            first, second = i.split(':')
            if first != second:
                yield ('reciprocal', (i))

    def steps(self):
        return [MRStep(mapper=self.mapper1,
                       reducer=self.reducer1),
                MRStep(mapper=self.mapper2,
                       reducer=self.reducer2),
                MRStep(mapper=self.mapper3,
                       reducer=self.reducer3)]


if __name__== '__main__':

    job = MRFindReciprocal(args=[])
    with open('enron_mails_small.csv', 'r') as fi:
        next(fi)
        output = list(mr.runJob(enumerate(map(lambda x: x.strip(), fi)), job))

        print(len(output))
        for i in output:
            print(i)