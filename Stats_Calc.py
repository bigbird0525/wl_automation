class Stats_Calc(object):

    def __init__(self):
        super().__init__()

    def getWeightedAvg(self, data):
        '''
        Calculates the weighted average for data
        :param data:
        :return:
        '''
        wAvg = 0
        for rows in data:
            if (rows['wins'] >= 50 and rows['conversions'] >= 2):
                wAvg += rows['percent']*rows['RPM']
        return wAvg

    def getAvg(self,data):
        '''
        Calculates average without taking weight of total wins
        :param data:
        :return:
        '''
        count = 0
        total = 0
        for rows in data:
            if (rows['wins'] >= 50 and rows['conversions'] >= 2):
                total += rows['RPM']
                count += 1

        avg = total/count
        return avg

    def getWeightedVariance(self, data, wAvg):
        '''
        Calculated variation on weighted average, using normalized total weight = 1
        :param data:
        :return:
        '''
        variance = 0
        for rows in data:
            if (rows['wins'] >= 50 and rows['conversions'] >= 2):
                variance += (rows['percent']*(rows['RPM']-wAvg)**2)
        return variance

    def getVariance(self,data,avg):
        '''
        Calculates variance without taking weight of total wins into consideration
        :param data:
        :param Avg:
        :return:
        '''
        variance = 0
        count = 0
        for rows in data:
            if (rows['wins'] >= 50 and rows['conversions'] >= 2):
                variance += ((rows['RPM']-avg)**2)
                count +=1
        variance = variance / count
        return variance

    def getStandardDeviation(self, variance):
        '''
        Calculates standard deviation of data
        :param data:
        :return:
        '''
        stDeviation = variance**.5
        return stDeviation
