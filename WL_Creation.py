from operator import itemgetter

class WL_Creation:

    def __init__(self):
        super().__init__()

    def getWeightedRPMrange(self, wAvg, stDev):
        '''
        Determines RPM ranges for each WL tier

        Dev notes: look into setting min value of 0 to adjust how the variance/stDev is calculated. In some cases, current
        logic produces a negative bid
        :param data:
        :return:
        '''
        bucketOne = wAvg + stDev
        if wAvg < .15:
            bucketTwo = 0
            bucketThree = 0
            return bucketOne, bucketTwo, bucketThree
        elif (wAvg - 2*stDev) < 0.15:
            bucketTwo = (wAvg - 0.15) * .25
            bucketThree = .15
            return bucketOne, bucketTwo, bucketThree
        else:
            bucketTwo = wAvg - stDev
            bucketThree = wAvg - 2*stDev
            return bucketOne, bucketTwo, bucketThree

    def getRPMrange(self, avg, stDev):
            '''
            Determines RPM ranges for each WL tier

            Dev notes: look into setting min value of 0 to adjust how the variance/stDev is calculated. In some cases, current
            logic produces a negative bid
            :param data:
            :return:
            '''
            if avg < .15:
                bucketOne = 0
                bucketTwo = 0
                bucketThree = 0
            elif stDev >= (avg - .25):
                bucketOne = avg
                bucketTwo = bucketOne - (bucketOne/6+.15)
                bucketThree = bucketOne - (bucketOne/2+.15)
            else:
                bucketOne = avg
                bucketTwo = avg - stDev/4
                bucketThree = avg - stDev/2

            if bucketTwo < .15:
                bucketTwo = .15
                bucketThree = 0

            elif bucketThree < .15:
                bucketThree = .15

            return bucketOne, bucketTwo, bucketThree

    def createNewWL(self, new_data, currentWL, bucketOne, bucketTwo, bucketThree):
        '''
        Creates a new WL with new performance as well as adding old subids that didn't meet minimum traffic standards
        :param data:
        :return:
        '''
        tier1 = []
        tier2 = []
        tier3 = []
        outliers_tmp = []
        tier1_name = {}
        tier2_name = {}
        tier3_name = {}

        for rows in currentWL:
            if 'Tier1' in rows['name']:
                tier1.append(rows)
            elif 'Tier2' in rows['name']:
                tier2.append(rows)
            elif 'Tier3' in rows['name']:
                tier3.append(rows)

        for rows in new_data:
            if (rows['wins'] < 50 and rows['conversions'] >= 2) or (rows['RPM'] >= 15.0 and rows['wins'] >= 50):
                outliers_tmp.append(rows)

        for rows in new_data:
            if any(rows == x for x in outliers_tmp):
                continue
            else:
                if 'Tier1' in rows['name']:
                    tier1_name['line_item_id'] = rows['line_item_id']
                    tier1_name['name'] = rows['name']
                elif 'Tier2' in rows['name']:
                    tier2_name['line_item_id'] = rows['line_item_id']
                    tier2_name['name'] = rows['name']
                elif 'Tier3' in rows['name']:
                    tier3_name['line_item_id'] = rows['line_item_id']
                    tier3_name['name'] = rows['name']

        for rows in new_data:
            if (rows['wins'] >= 50 and rows['conversions'] >= 2 and rows['RPM'] < 15):
                if rows['RPM'] > bucketOne:
                    tier1.append(rows)
                elif (bucketTwo > 0 and (rows['RPM'] <= bucketOne and rows['RPM'] > bucketTwo)):
                    tier2.append(rows)
                elif (bucketThree > 0 and (rows['RPM'] <= bucketTwo and rows['RPM'] > bucketThree)):
                    tier3.append(rows)
                else:
                    continue
            else:
                continue

        tier1 = WL_Creation().cleanNewWL(tier1, tier1_name['line_item_id'], tier1_name['name'])
        tier2 = WL_Creation().cleanNewWL(tier2, tier2_name['line_item_id'], tier2_name['name'])
        tier3 = WL_Creation().cleanNewWL(tier3, tier3_name['line_item_id'], tier3_name['name'])
        return tier1, tier2, tier3, outliers_tmp

    def cleanNewWL(self, wl, id, name):
        '''
        Takes in new WL and cleans up the data for our upload script. It updates the line item to the
        appropriate tier, and then dedupes the data to ensure there are no duplicates.
        :param wl:
        :param id_name:
        :return:
        '''
        new_WL = []
        for rows in wl:
            temp = {}
            temp['line_item_id'] = id
            temp['name'] = name
            temp['country_iso'] = rows['country_iso']
            temp['os'] = rows['os']
            temp['subid'] = rows['subid']
            temp['RPM'] = rows['RPM']
            temp['revenue_estimate'] = rows['revenue_estimate']
            temp['conversions'] = rows['conversions']
            if any(temp['subid'] == x['subid'] for x in new_WL):
                continue
            else:
                new_WL.append(temp)
        return new_WL

    def createNewWL_v2(self, data):
        '''
        data - performance data
        line_items - list of dictionary
        :param data:
        :param line_items:
        :return:
        '''
        wl = []
        tmp_wl = [[],[],[]]
        outliers = []
        total_revenue = 0
        tier1_name = {}
        tier2_name = {}
        tier3_name = {}
        ordered_data = sorted(data, key=itemgetter('RPM'), reverse=True)
        for rows in data:
            if (rows['wins'] < 50 and rows['conversions'] >= 2) or (rows['RPM'] >= 15.0 and rows['wins'] >= 50):
                outliers.append(rows)

        for rows in data:
            if any(rows == x for x in outliers):
                continue
            else:
                if 'Tier1' in rows['name']:
                    tier1_name['line_item_id'] = rows['line_item_id']
                    tier1_name['name'] = rows['name']
                elif 'Tier2' in rows['name']:
                    tier2_name['line_item_id'] = rows['line_item_id']
                    tier2_name['name'] = rows['name']
                elif 'Tier3' in rows['name']:
                    tier3_name['line_item_id'] = rows['line_item_id']
                    tier3_name['name'] = rows['name']

        for rows in data:
            total_revenue += rows['revenue_estimate']

        rev_eval = total_revenue/4
        print("Total revenue for this WL eval is " + str(total_revenue))
        print("Test Rev_Eval number is: "+str(rev_eval))

        for wls in tmp_wl:
            while True:
                tmp_rev = 0
                for rows in ordered_data:
                    if any(rows == y for y in wls):
                        continue
                    elif tmp_rev >= rev_eval:
                        break
                    else:
                        wls.append(rows)
                        tmp_rev += rows['revenue_estimate']
                ordered_data = [x for x in ordered_data if x not in wls]
                break
        wl.extend(({'tier1_data': tmp_wl[0]},{'tier2_data': tmp_wl[1]},{'tier3_data': tmp_wl[2]}))
        if not tier1_name:
            tier1_name['line_item_id'] = 'No Current Tier 1 Campaign'
            tier1_name['name'] = 'No Current Tier 1 Campaign'
        if not tier2_name:
            tier2_name['line_item_id'] = 'No Current Tier 2 Campaign'
            tier2_name['name'] = 'No Current Tier 2 Campaign'
        if not tier3_name:
            tier3_name['line_item_id'] = 'No Current Tier 3 Campaign'
            tier3_name['name'] = 'No Current Tier 3 Campaign'
        tier1_wl = WL_Creation().cleanNewWL(wl[0]['tier1_data'], tier1_name['line_item_id'], tier1_name['name'])
        tier2_wl = WL_Creation().cleanNewWL(wl[1]['tier2_data'], tier2_name['line_item_id'], tier2_name['name'])
        tier3_wl = WL_Creation().cleanNewWL(wl[2]['tier3_data'], tier3_name['line_item_id'], tier3_name['name'])

        return tier1_wl, tier2_wl, tier3_wl, outliers

