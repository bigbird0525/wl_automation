'''
Written by Andrew Ravn
Created on: 27 JUL 2018
Last Updated: 8 AUG 2018

'''

import bidder_optimizations.MySQL_Connector as mSQL
import bidder_optimizations.Stats_Calc as Stats
import bidder_optimizations.WL_Creation as WL
import bidder_optimizations.ReportWriter as RW
import bidder_optimizations.uploadWL as uWL
from datetime import date

class Optimizations:

    def __init__(self):
        super().__init__()

    def getData(self):
        '''
        Collects performance data on bidder campaigns
        :return:
        '''
        query = '''SELECT
        r.line_item_id,
        li.name,
        r.country_iso,
        r.os,
        r.bundle as bundle,
        concat(r.exchange_publisher_id,'_',r.bundle) as subid,
          sum(r.bids) as bids,
          sum(r.wins) as wins,
          sum(r.impressions) as impressions,
          sum(r.conversions) as conversions,
          li.budget_daily as budget,
          sum(r.expense_microdollars) / 1000000 as spend,
          sum(r.revenue) as revenue_estimate,
          sum(r.revenue) - sum(r.expense_microdollars) / 1000000  as profit ,
          (sum(r.revenue) - sum(r.expense_microdollars) / 1000000) / sum(r.revenue) as margin,
          sum(r.revenue) / sum(r.wins)*1000 as RPM,
          (sum(r.expense_microdollars) / 1000000 )/sum(r.wins)*1000 as cost_cpm
          FROM rtb_stats_demand_hour_mdt r
          join line_item li on li.id = r.line_item_id
        WHERE r.day >= CURDATE() - INTERVAL 4 DAY
        AND r.day < CURDATE()
        AND r.line_item_id in (
        22767211
        ,24430714
        ,27180648
        ,27186351
        ,28026262
        ,21871142
        ,21871143
        ,21871144
        ,27180551
        ,28026263
        ,28156611
        )
        GROUP BY 1,2,3,4,5
        order by spend desc;
        '''
        data = mSQL.MySQL_Connector("{0}").query(query) # Input connection information such as username, password, etc. See MySQL_Connector method for more details
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


    def getCurrentWL(self, line_item_id, perf_data):
        '''
        Retrieves current WL
        :param line_item_id:
        :return:
        '''
        current_WL = []
        query = '''
        SELECT line_item.id, line_item.name, attribute_type.key, group_concat(attribute.key) attributes
        FROM line_item
        JOIN attribute_target ON attribute_target.target_id = line_item.target_id
        JOIN attribute ON attribute.id = attribute_target.attribute_id
        JOIN attribute_type ON attribute_type.id = attribute.attribute_type_id
        WHERE line_item.id = {0} and attribute_type.key = 'source'
        GROUP BY 1, 2, 3;'''.format(line_item_id)
        data = mSQL.MySQL_Connector("{0}").query(query) # See MySQL_Connector


        if not data:
            query = '''
            SELECT line_item.id, line_item.name, attribute_type.key, group_concat(attribute.key) attributes
            FROM line_item
            JOIN attribute_target ON attribute_target.target_id = line_item.target_id
            JOIN attribute ON attribute.id = attribute_target.attribute_id
            JOIN attribute_type ON attribute_type.id = attribute.attribute_type_id
            WHERE line_item.id = {0} and attribute_type.key = 'bundle'
            GROUP BY 1, 2, 3;'''.format(line_item_id)
            data = mSQL.MySQL_Connector("{0}").query(query) # See MySQL_Connector
        for rows in data:
            attributes_list = []
            target_list = rows['attributes'].split(',')
            for attributes in target_list:
                 temp = {}
                 if any(attributes == x for x in attributes_list):
                     continue
                 elif any(attributes == x['subid'] for x in perf_data):
                     for items in perf_data:
                        if attributes == items['subid']:
                            current_WL.append(items)
                            attributes_list.append(attributes)
                 else:
                     temp['line_item_id'] = rows['id']
                     temp['name'] = rows['name']
                     temp['country_iso'] = perf_data[0]['country_iso']
                     temp['os'] = perf_data[0]['os']
                     temp['subid'] = attributes
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

def main():

    cur_time = date.strftime(date.today(), '%m_%d_%Y')
    opt_object = Optimizations()
    performance_data = opt_object.getData()
    RW.ReportWriter().writer(performance_data, 'performance_report_'+cur_time)    # Write report with performance data before slicing and manipulation
    geoOScombos = opt_object.getUniqueOSGeoCombos(performance_data)             # Returns list of Geo Platform combinations ("Geo_Platform")

    # split out the Geo Platform combinations and slice raw data by Geo/Platform
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
        # Calculate statistic values
        # avg = Stats.Stats_Calc().getAvg(sliced_data)
        # variance = Stats.Stats_Calc().getVariance(sliced_data,avg)
        # stDev = Stats.Stats_Calc().getStandardDeviation(variance)
        # bucketOne, bucketTwo, bucketThree = WL.WL_Creation().getRPMrange(avg, stDev)
        # print("avg: " + str(avg) + "\nvariance: " + str(variance) + "\nstandard deviation: " + str(stDev))
        # print("Bucket One: " + str(bucketOne))
        # print("Bucket Two: " + str(bucketTwo))
        # print("Bucket Three: " + str(bucketThree))

        consolidated_data, low_volume_ids = [],[]

        # Split out line item ids to pull down current WL and then write new WL
        for rows in optinLineItem:
            lineItemSlicedData = []

            for items in sliced_data:

                if items['line_item_id'] == rows:
                    lineItemSlicedData.append(items)
                    if items['conversions'] >= 2:
                        consolidated_data.append(items)

            currentWL = opt_object.getCurrentWL(rows, lineItemSlicedData)

            for entries in currentWL:
                if 'Tier1' in entries['name'] and (entries['wins'] < 50 and entries['conversions'] < 2):
                    low_volume_ids.append(entries)
                elif 'Tier2' in entries['name']and (entries['wins'] < 50 and entries['conversions'] < 2):
                    low_volume_ids.append(entries)
                elif 'Tier3' in entries['name']and (entries['wins'] < 50 and entries['conversions'] < 2):
                    low_volume_ids.append(entries)


        #tier1_wl, tier2_wl, tier3_wl, outliers_ls = WL.WL_Creation().createNewWL(consolidated_data, low_volume_ids,bucketOne, bucketTwo, bucketThree)
        tier1_wl, tier2_wl, tier3_wl, outliers_ls = WL.WL_Creation().createNewWL_v2(consolidated_data)
        t1_rpm = []
        t1_rev = 0
        t2_rpm = []
        t2_rev = 0
        t3_rpm = []
        t3_rev = 0
        for rows in tier1_wl:
            t1_rpm.append(rows['RPM'])
            t1_rev += rows['revenue_estimate']
        for rows in tier2_wl:
            t2_rpm.append(rows['RPM'])
            t2_rev += rows['revenue_estimate']
        for rows in tier3_wl:
            t3_rpm.append(rows['RPM'])
            t3_rev += rows['revenue_estimate']


        try:
            print("Tier 1 RPM range is $" + str(round(min(t1_rpm),2)) + " to $"+ str(round(max(t1_rpm),2))+" and total number of subids is "+str(len(t1_rpm)))
        except ValueError:
            print("Tier 1 is empty")
        try:
            print("Tier 2 RPM range is $" + str(round(min(t2_rpm),2)) + " to $"+str(round(max(t2_rpm),2))+" and total number of subids is "+str(len(t2_rpm)))
        except ValueError:
            print("Tier 2 is empty")
        try:
            print("Tier 3 RPM range is $" + str(round(min(t3_rpm),2)) + " to $"+str(round(max(t3_rpm),2))+" and total number of subids is "+str(len(t3_rpm)))
        except ValueError:
            print("Tier 3 is empty")
        print("\n Would you like to see the Tier 1 List?")
        response = input(">>> ")
        if response.strip().lower() == 'yes':
            print("Tier1 List: \n")
            for rows in tier1_wl:
                print(rows)
        print("\n Would you like to see the Tier 2 List?")
        response = input(">>> ")
        if response.strip().lower() == 'yes':
            print("\nTier2 List: \n")
            for rows in tier2_wl:
                print(rows)
        print("\n Would you like to see the Tier 3 List?")
        response = input(">>> ")
        if response.strip().lower() == 'yes':
            print("\nTier3 List: \n")
            for rows in tier3_wl:
                print(rows)
        print("\n\n\n Would you like to write this to the DB? Yes or No?")
        response = input(">>> ")
        if response.strip().lower() == 'yes':
            if len(low_volume_ids) > 0:
                RW.ReportWriter().writer(low_volume_ids, "low_volume_report_"+geo+"_"+os+"_"+cur_time)
            if len(tier1_wl) > 0:
                RW.ReportWriter().writer(tier1_wl, "tier1_wl_"+geo+"_"+os+"_"+cur_time)
            if len(tier2_wl) > 0:
                RW.ReportWriter().writer(tier2_wl, "tier2_wl_"+geo+"_"+os+"_"+cur_time)
            if len(tier3_wl) > 0:
                RW.ReportWriter().writer(tier3_wl, "tier3_wl_"+geo+"_"+os+"_"+cur_time)
            if len(outliers_ls) > 0:
                RW.ReportWriter().writer(outliers_ls,"outliers_ls_"+geo+"_"+os+"_"+cur_time)
        else:
            break

if __name__ == '__main__':
    main()
    print("Done")
