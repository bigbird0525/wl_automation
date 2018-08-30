import csv, os

class ReportWriter(object):


    def writer(self, data, report_name):
        '''
        Writes report showing subids that aren't receiving enough traffic to be evaluated but are on WLs
        :param data:
        :return:
        '''
        try:
            with open(os.getcwd() + "/" + report_name + ".csv", 'w') as report:
                csv_writer = csv.DictWriter(report, data[0].keys())
                csv_writer.writeheader()
                csv_writer.writerows(data)
        except ValueError:
            print("Something didn't work right with the report writer")
