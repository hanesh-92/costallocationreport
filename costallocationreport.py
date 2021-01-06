import os
import boto3
import datetime
import logging
#import pandas as pd
# For date
from dateutil.relativedelta import relativedelta

CURRENT_MONTH = os.environ.get('CURRENT_MONTH')
if CURRENT_MONTH == "true":
    CURRENT_MONTH = True
else:
    CURRENT_MONTH = False


# Default exclude support, as for Enterprise Support
# as support billing is finalised later in month so skews trends
INC_SUPPORT = os.environ.get('INC_SUPPORT')
if INC_SUPPORT == "true":
    INC_SUPPORT = True
else:
    INC_SUPPORT = False

TAG_VALUE_FILTER = os.environ.get('TAG_VALUE_FILTER') or '*'
TAG_KEY = os.environ.get('TAG_KEY')


class CostExplorer:
    """Retrieves BillingInfo checks from CostExplorer API
    >>> costexplorer = CostExplorer()
    >>> costexplorer.addReport(GroupBy=[{"Type": "DIMENSION","Key": "SERVICE"}])
    >>> costexplorer.generateExcel()
    """

    def __init__(self, CurrentMonth=False):
        # Array of reports ready to be output to Excel.
        self.reports = []
        self.client = boto3.client('ce', region_name='us-east-1')
        self.end = datetime.date.today().replace(day=1)
        self.riend = datetime.date.today()
        if CurrentMonth or CURRENT_MONTH:
            self.end = self.riend
        else:
            # Default is last 12 months
            self.start = (datetime.date.today() - relativedelta(months=+12)).replace(
                day=1)  # 1st day of month 12 months ago

        self.ristart = (datetime.date.today() - relativedelta(months=+11)).replace(
            day=1)  # 1st day of month 11 months ago

        try:
            self.accounts = self.getAccounts()
        except:
            logging.exception("Getting Account names failed")
            self.accounts = {}

    def getAccounts(self):
        accounts = {}
        client = boto3.client('organizations', region_name='us-east-1')
        paginator = client.get_paginator('list_accounts')
        response_iterator = paginator.paginate()
        for response in response_iterator:
            for acc in response['Accounts']:
                accounts[acc['Id']] = acc
        return accounts

    def addRiReport(self, Name='RICoverage', Savings=False, PaymentOption='PARTIAL_UPFRONT',
                    Service='Amazon Elastic Compute Cloud - Compute'):  # Call with Savings True to get Utilization report in dollar savings
        type = 'chart'  # other option table
        if Name == "RICoverage":
            results = []
            response = self.client.get_reservation_coverage(
                TimePeriod={
                    'Start': self.ristart.isoformat(),
                    'End': self.riend.isoformat()
                },
                Granularity='DAILY'
            )
            results.extend(response['CoveragesByTime'])
            while 'nextToken' in response:
                nextToken = response['nextToken']
                response = self.client.get_reservation_coverage(
                    TimePeriod={
                        'Start': self.ristart.isoformat(),
                        'End': self.riend.isoformat()
                    },
                    Granularity='DAILY',
                    NextPageToken=nextToken
                )
                results.extend(response['CoveragesByTime'])
                if 'nextToken' in response:
                    nextToken = response['nextToken']
                else:
                    nextToken = False

            rows = []
            for v in results:
                row = {'date': v['TimePeriod']['Start']}
                row.update({'Coverage%': float(v['Total']['CoverageHours']['CoverageHoursPercentage'])})
                rows.append(row)

            df = pd.DataFrame(rows)
            df.set_index("date", inplace=True)
            df = df.fillna(0.0)
            df = df.T
        elif Name in ['RIUtilization', 'RIUtilizationSavings']:
            # Only Six month to support savings
            results = []
            response = self.client.get_reservation_utilization(
                TimePeriod={
                    'Start': self.start.isoformat(),
                    'End': self.riend.isoformat()
                },
                Granularity='DAILY'
            )
            results.extend(response['UtilizationsByTime'])
            while 'nextToken' in response:
                nextToken = response['nextToken']
                response = self.client.get_reservation_utilization(
                    TimePeriod={
                        'Start': self.ristart.isoformat(),
                        'End': self.riend.isoformat()
                    },
                    Granularity='DAILY',
                    NextPageToken=nextToken
                )
                results.extend(response['UtilizationsByTime'])
                if 'nextToken' in response:
                    nextToken = response['nextToken']
                else:
                    nextToken = False

            rows = []
            if results:
                for v in results:
                    row = {'date': v['TimePeriod']['Start']}
                    if Savings:
                        row.update({'Savings$': float(v['Total']['NetRISavings'])})
                    else:
                        row.update({'Utilization%': float(v['Total']['UtilizationPercentage'])})
                    rows.append(row)

                df = pd.DataFrame(rows)
                df.set_index("date", inplace=True)
                df = df.fillna(0.0)
                df = df.T
                type = 'chart'
            else:
                df = pd.DataFrame(rows)
                type = 'table'  # Dont try chart empty result
        elif Name == 'RIRecommendation':
            results = []

            rows = []
            for i in results:
                for v in i['RecommendationDetails']:
                    row = v['InstanceDetails'][list(v['InstanceDetails'].keys())[0]]
                    row['Recommended'] = v['RecommendedNumberOfInstancesToPurchase']
                    row['Minimum'] = v['MinimumNumberOfInstancesUsedPerHour']
                    row['Maximum'] = v['MaximumNumberOfInstancesUsedPerHour']
                    row['Savings'] = v['EstimatedMonthlySavingsAmount']
                    row['OnDemand'] = v['EstimatedMonthlyOnDemandCost']
                    row['BreakEvenIn'] = v['EstimatedBreakEvenInMonths']
                    row['UpfrontCost'] = v['UpfrontCost']
                    row['MonthlyCost'] = v['RecurringStandardMonthlyCost']
                    rows.append(row)

            df = pd.DataFrame(rows)
            df = df.fillna(0.0)
            type = 'table'  # Dont try chart this
        self.reports.append({'Name': Name, 'Data': df, 'Type': type})

    def addLinkedReports(self, Name='RI_{}', PaymentOption='PARTIAL_UPFRONT'):
        pass

    def addReport(self, Name="Default", GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}, ],
                  Style='Total', NoCredits=True, CreditsOnly=False, RefundOnly=False, UpfrontOnly=False,
                  IncSupport=False):
        type = 'chart'  # other option table
        results = []
        if not NoCredits:
            response = self.client.get_cost_and_usage(
                TimePeriod={
                    'Start': self.start.isoformat(),
                    'End': self.end.isoformat()
                },
                Granularity='DAILY',
                Metrics=[
                    'UnblendedCost',
                ],
                GroupBy=GroupBy
            )
        else:
            Filter = {"And": []}

            Dimensions = {
                "Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund", "Upfront", "Support"]}}}
            if INC_SUPPORT or IncSupport:  # If global set for including support, we dont exclude it
                Dimensions = {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund", "Upfront"]}}}
            if CreditsOnly:
                Dimensions = {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", ]}}
            if RefundOnly:
                Dimensions = {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Refund", ]}}
            if UpfrontOnly:
                Dimensions = {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Upfront", ]}}

            tagValues = None
            if TAG_KEY:
                tagValues = self.client.get_tags(
                    SearchString=TAG_VALUE_FILTER,
                    TimePeriod={
                        'Start': self.start.isoformat(),
                        'End': datetime.date.today().isoformat()
                    },
                    TagKey=TAG_KEY
                )

            if tagValues:
                Filter["And"].append(Dimensions)
                if len(tagValues["Tags"]) > 0:
                    Tags = {"Tags": {"Key": TAG_KEY, "Values": tagValues["Tags"]}}
                    Filter["And"].append(Tags)
            else:
                Filter = Dimensions.copy()

            response = self.client.get_cost_and_usage(
                TimePeriod={
                    'Start': self.start.isoformat(),
                    'End': self.end.isoformat()
                },
                Granularity='DAILY',
                Metrics=[
                    'UnblendedCost',
                ],
                GroupBy=GroupBy,
                Filter=Filter
            )

        if response:
            results.extend(response['ResultsByTime'])

            while 'nextToken' in response:
                nextToken = response['nextToken']
                response = self.client.get_cost_and_usage(
                    TimePeriod={
                        'Start': self.start.isoformat(),
                        'End': self.end.isoformat()
                    },
                    Granularity='DAILY',
                    Metrics=[
                        'UnblendedCost',
                    ],
                    GroupBy=GroupBy,
                    NextPageToken=nextToken
                )

                results.extend(response['ResultsByTime'])
                if 'nextToken' in response:
                    nextToken = response['nextToken']
                else:
                    nextToken = False
        rows = []
        sort = ''
        for v in results:
            row = {'date': v['TimePeriod']['Start']}
            sort = v['TimePeriod']['Start']
            for i in v['Groups']:
                key = i['Keys'][0]
                if key in self.accounts:
                    key = self.accounts[key]
                row.update({key: float(i['Metrics']['UnblendedCost']['Amount'])})
            if not v['Groups']:
                row.update({'Total': float(v['Total']['UnblendedCost']['Amount'])})
            rows.append(row)

        df = pd.DataFrame(rows)
        df.set_index("date", inplace=True)
        df = df.fillna(0.0)

        if Style == 'Change':
            dfc = df.copy()
            lastindex = None
            for index, row in df.iterrows():
                if lastindex:
                    for i in row.index:
                        try:
                            df.at[index, i] = dfc.at[index, i] - dfc.at[lastindex, i]
                        except:
                            logging.exception("Error")
                            df.at[index, i] = 0
                lastindex = index
        df = df.T
        df = df.sort_values(sort, ascending=False)
        self.reports.append({'Name': Name, 'Data': df, 'Type': type})

    def generateExcel(self):
        # Create a Pandas Excel writer using XlsxWriter as the engine.\
        os.chdir('/tmp')
        writer = pd.ExcelWriter('cost_explorer_report.xlsx', engine='xlsxwriter')
        workbook = writer.book
        for report in self.reports:
            print(report['Name'])
            report['Data'].to_excel(writer, sheet_name=report['Name'])
            worksheet = writer.sheets[report['Name']]

        # Time to deliver the file to S3
        if os.environ.get('S3_BUCKET'):
            s3 = boto3.client('s3')
            s3.upload_file("cost_explorer_report.xlsx", os.environ.get('S3_BUCKET'), "cost_explorer_report.xlsx")

def lambda_handler(event, context):
    costexplorer = CostExplorer(CurrentMonth=False)
    # Default addReport has filter to remove Support / Credits / Refunds / UpfrontRI
    # Overall Billing Reports
    costexplorer.addReport(Name="Total", GroupBy=[], Style='Total', IncSupport=True)
    costexplorer.addReport(Name="TotalChange", GroupBy=[], Style='Change')
    costexplorer.addReport(Name="TotalInclCredits", GroupBy=[], Style='Total', NoCredits=False, IncSupport=True)
    costexplorer.addReport(Name="TotalInclCreditsChange", GroupBy=[], Style='Change', NoCredits=False)
    costexplorer.addReport(Name="Credits", GroupBy=[], Style='Total', CreditsOnly=True)
    costexplorer.addReport(Name="Refunds", GroupBy=[], Style='Total', RefundOnly=True)
    costexplorer.addReport(Name="RIUpfront", GroupBy=[], Style='Total', UpfrontOnly=True)
    # GroupBy Reports
    costexplorer.addReport(Name="Services", GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}], Style='Total',
                           IncSupport=True)
    costexplorer.addReport(Name="ServicesChange", GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}], Style='Change')
    costexplorer.addReport(Name="Accounts", GroupBy=[{"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"}], Style='Total')
    costexplorer.addReport(Name="AccountsChange", GroupBy=[{"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"}],
                           Style='Change')
    costexplorer.addReport(Name="Regions", GroupBy=[{"Type": "DIMENSION", "Key": "REGION"}], Style='Total')
    costexplorer.addReport(Name="RegionsChange", GroupBy=[{"Type": "DIMENSION", "Key": "REGION"}], Style='Change')
    if os.environ.get('COST_TAGS'):  # Support for multiple/different Cost Allocation tags
        for tagkey in os.environ.get('COST_TAGS').split(','):
            tabname = tagkey.replace(":", ".")  # Remove special chars from Excel tabname
            costexplorer.addReport(Name="{}".format(tabname)[:31], GroupBy=[{"Type": "TAG", "Key": tagkey}],
                                   Style='Total')
            costexplorer.addReport(Name="Change-{}".format(tabname)[:31], GroupBy=[{"Type": "TAG", "Key": tagkey}],
                                   Style='Change')
    # RI Reports
    costexplorer.addRiReport(Name="RICoverage")
    costexplorer.addRiReport(Name="RIUtilization")
    costexplorer.addRiReport(Name="RIUtilizationSavings", Savings=True)
    costexplorer.addRiReport(
        Name="RIRecommendation")  # Service supported value(s): Amazon Elastic Compute Cloud - Compute, Amazon Relational Database Service
    costexplorer.generateExcel()
    return "Report Generated"


if __name__ == '__main__':
    lambda_handler()
