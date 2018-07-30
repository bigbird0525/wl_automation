'''
Written by Andrew Ravn
Last Updated: 27 JUL

Creates new WLs for Tiered campaigns for bidder. Calculates weighted average and standard deviation. Tier 1 > +1 std, -1 std <= Tier 2 <= +1 std, -2 std =< Tier 3 < -1 std
'''

import bidder_optimizations.MySQL_Connector as mSQL
import os, csv
from datetime import date

class Optimizations:

    def __init__(self):
        super().__init__()

    def getData(self):
        '''
        Collects performance data on bidder campaigns
        :return:
        '''
        query = '''Insert query pulling down current subid performance with RPMs calculated
        '''

        data = mSQL.MySQL_Connector().query(query)
        for rows in data:
            if (rows['RPM'] is None or not rows['RPM']):
                rows['RPM'] = 0.0
            if (rows['margin'] is None or not rows['margin']):
                rows['margin'] = 0.0
            if (rows['cost_cpm'] is None or not rows['cost_cpm']):
                rows['cost_cpm'] = 0.0
        return data

    def getUniqueOSGeoCombos(self, data):
        '''
        Finds all unique Geo/OS combinations in data
        :param data:
        :return:
        '''
        temp_list = []
        for rows in data:
            temp = rows['country_iso'] +'_'+rows['os']
            temp_list.append(temp)

        geoOS_combos = set(temp_list)
        return geoOS_combos

    def getGeoOSSlices(self, data, os, geo):
        '''
        Slices data by Geo and OS for calculations and manipulation
        :param data:
        :return:
        '''
        sliced_data = []
        total_wins = 0
        for rows in data:
            if rows['country_iso'] != geo:
                continue
            elif rows['os'] != os:
                continue
            else:
                sliced_data.append(rows)
                total_wins += rows['wins']

        for rows in sliced_data:
            if total_wins > 0:
                rows['RPM'] = float(rows['RPM'])
                rows['percent'] = float(rows['wins']/total_wins)
            else:
                rows['percent'] = 0.0
        return sliced_data

    def getWeightedAvg(self, data):
        '''
        Calculates the weighted average for data
        :param data:
        :return:
        '''
        wAvg = 0
        for rows in data:
            wAvg = rows['percent']*rows['RPM']
        return wAvg

    def getVariance(self, data, wAvg):
        '''
        Calculated variation on weighted average, using normalized total weight = 1
        :param data:
        :return:
        '''
        variance = 0
        for rows in data:
            variance += rows['percent']*(rows['RPM']-wAvg)**2
        return variance

    def getStandardDeviation(self, variance):
        '''
        Calculates standard deviation of data
        :param data:
        :return:
        '''
        stDeviation = variance**.5
        return stDeviation

    def getRPMrange(self,wAvg, stDev):
        '''
        Determines RPM ranges for each WL tier

        Dev notes: look into setting min value of 0 to adjust how the variance/stDev is calculated. In some cases, current
        logic produces a negative bid
        :param data:
        :return:
        '''
        weight = .5
        adjStDev = stDev
        if wAvg > 0.25:
            while (wAvg - stDev <= 0.15):
                adjStDev -= adjStDev * weight
                weight += .05

        bucketOne = wAvg + stDev
        bucketTwo = wAvg - adjStDev
        bucketThree = wAvg - (2*adjStDev)

        if bucketTwo < .15:
            bucketTwo = 0

        if bucketThree < .15:
            bucketThree = 0

        return bucketOne, bucketTwo, bucketThree

    def getCurrentWL(self, line_item_id, perf_data):
        '''
        Retrieves current WL
        :param line_item_id:
        :return:
        '''
        current_WL = []
        query = '''
        Insert query for current WL with line item id where clause'''.format(line_item_id)
       
        data = mSQL.MySQL_Connector().query(query)
        for rows in data:
            attributes_list = []
            target_list = rows['attributes'].split(',')

            for attributes in target_list:
                 temp = {}
                 if any(attributes == x for x in attributes_list):
                     continue
                 elif any(attributes == x['bundle'] for x in perf_data):
                     for items in perf_data:
                        if attributes == items['bundle']:
                            current_WL.append(items)
                            attributes_list.append(attributes)
                 else:
                     temp['line_item_id'] = rows['id']
                     temp['name'] = rows['name']
                     temp['country_iso'] = perf_data[0]['country_iso']
                     temp['os'] = perf_data[0]['os']
                     temp['bundle'] = attributes
                     temp['bids'] = 0.0
                     temp['wins'] = 0.0
                     temp['impressions'] = 0.0
                     temp['conversions'] = 0.0
                     temp['budget'] = 0.0
                     temp['spend'] = 0.0
                     temp['revenue_estimate'] = 0.0
                     temp['profit'] = 0.0
                     temp['margin'] = 0.0
                     temp['RPM'] = 0.0
                     temp['cost_cpm'] = 0.0
                     temp['percent'] = 0.0
                     current_WL.append(temp)
                     attributes_list.append(attributes)
        return current_WL

    def createNewWL(self, new_data, currentWL, bucketOne, bucketTwo, bucketThree):
        '''
        Creates a new WL with new performance as well as adding old subids that didn't meet minimum traffic standards
        :param data:
        :return:
        '''
        tier1 = []
        tier2 = []
        tier3 = []

        for rows in currentWL:
            if 'Tier1' in rows['name']:
                tier1.append(rows)
            elif 'Tier2' in rows['name']:
                tier2.append(rows)
            elif 'Tier3' in rows['name']:
                tier3.append(rows)

        for rows in new_data:
            if (rows['wins'] > 100 and rows['conversions'] > 2):
                if rows['RPM'] > bucketOne:
                    tier1.append(rows)
                elif (bucketTwo > 0 and (rows['RPM'] <= bucketOne and rows['RPM'] > bucketTwo)):
                    tier2.append(rows)
                elif (bucketThree > 0 and (rows['RPM'] <= bucketTwo and rows['RPM' > bucketThree])):
                    tier3.append(rows)
                else:
                    continue
            else:
                continue
        return tier1, tier2, tier3

    def uploadNewWL(self, data):
        '''
        Deletes old WL and uploads new WL
        :param data:
        :return:
        '''
        pass

    def lowVolumeReportWriter(self, data, report_name):
        '''
        Writes report showing subids that aren't receiving enough traffic to be evaluated but are on WLs
        :param data:
        :return:
        '''
        with open(os.getcwd() + "/" + report_name + ".csv", 'w') as report:
            csv_writer = csv.DictWriter(report, data[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(data)

def main():

    cur_time = date.strftime(date.today(), '%m_%d_%Y')
    opt_object = Optimizations()
    performance_data = opt_object.getData()
    geoOScombos = opt_object.getUniqueOSGeoCombos(performance_data)
    for rows in geoOScombos:
        geo = rows.split("_")[0]
        os = rows.split("_")[1]
        sliced_data = opt_object.getGeoOSSlices(performance_data,os, geo)
        optinLineItem = []
        for rows in sliced_data:
            if any(rows['line_item_id'] == x for x in optinLineItem):
                continue
            else:
                optinLineItem.append(rows['line_item_id'])

        wAvg = opt_object.getWeightedAvg(sliced_data)
        variance = opt_object.getVariance(sliced_data,wAvg)
        stDev = opt_object.getStandardDeviation(variance)
        bucketOne, bucketTwo, bucketThree = opt_object.getRPMrange(wAvg, stDev)

        consolidated_data, low_volume_ids = [],[]

        for rows in optinLineItem:
            lineItemSlicedData = []

            for items in sliced_data:

                if items['line_item_id'] == rows:
                    lineItemSlicedData.append(items)
                    consolidated_data.append(items)

            currentWL = opt_object.getCurrentWL(rows, lineItemSlicedData)

            for entries in currentWL:

                if entries['wins'] < 100:
                    low_volume_ids.append(entries)

        tier1_wl, tier2_wl, tier3_wl = opt_object.createNewWL(consolidated_data, low_volume_ids,bucketOne, bucketTwo, bucketThree)
        if len(low_volume_ids) > 0:
            opt_object.lowVolumeReportWriter(low_volume_ids, "low_volume_report"+geo+"_"+os+"_"+cur_time)
        if len(tier1_wl) > 0:
            opt_object.lowVolumeReportWriter(tier1_wl, "tier1_wl_"+geo+"_"+os+"_"+cur_time)
        if len(tier2_wl) > 0:
            opt_object.lowVolumeReportWriter(tier2_wl, "tier2_wl_"+geo+"_"+os+"_"+cur_time)
        if len(tier3_wl) > 0:
            opt_object.lowVolumeReportWriter(tier3_wl, "tier3_wl_"+geo+"_"+os+"_"+cur_time)


if __name__ == '__main__':
    main()
    print("Done")
