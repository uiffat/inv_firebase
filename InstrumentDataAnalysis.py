import pandas as pd
import numpy as np
import holidays
import datetime


class InvFundData:

    def __init__(self, file_dir):
        self.file_dir = file_dir
        self.df = pd.read_csv(self.file_dir)

    def df_facts(self):
        print("Number of rows and columns in the data :", self.df.shape)
        print("Column names :", self.df.columns)
        num_data_points = self.df.groupby(['Subfund_Code', 'Share_Class_Code']).size()
        mean_NAV_per_share = self.df.groupby(['Subfund_Code', 'Share_Class_Code'])['NAV_Per_Share'].mean()
        print("Count of Data points by Sub funds and share classes present :",
              num_data_points)
        print("Mean NAV_Per_share :", mean_NAV_per_share)

        return num_data_points.to_dict(), mean_NAV_per_share.to_dict()

    def missingInstrumentValuations(self):
        """
        Checks for missing valuation data by dates for each Subfund and Share class combination
        :return: None
        """
        self.df['Valuation_Date'] = pd.to_datetime(self.df.Valuation_Date)
        instruments = self.df.groupby(['Subfund_Code', 'Share_Class_Code'])

        instrument_nm = []
        num_days_missing = []
        for name, group in instruments:
            print("\nInstrument - (Subbfund, Share_Class) : ", name)
            start_dt = min(group['Valuation_Date'])
            end_dt = max(group['Valuation_Date'])

            num_weekdays = np.busday_count(start_dt.date(), end_dt.date())
            lux_hols = holidays.Luxembourg()[start_dt:end_dt]
            lux_hols = [h for h in lux_hols if h.weekday() < 5]

            num_working_days = num_weekdays - len(lux_hols)

            n = num_working_days - group.shape[0]
            if n > 0:
                print("Valuation data is missing for {n_days} days between {s_dt} and {e_dt}".format(n_days=n,
                                                                                                     s_dt=start_dt.date(),
                                                                                                     e_dt=end_dt.date()))
            else:
                print("No valuation data missing")

            instrument_nm.append(name)
            num_days_missing.append(n)

        return dict(zip(instrument_nm, num_days_missing))

    def consistent_CCY(self, ccy_col_name):
        """
        Check if currency of the listed amounts is consistent
        :param ccy_col_name: column name to work with
        :return:
        """
        g = self.df.groupby(['Subfund_Code', 'Share_Class_Code'])[ccy_col_name].unique()

        return g.to_dict()

    @staticmethod
    def dates_generator(startDate, endDate):
        """
        Generates a list of working days between two given dates, removing weekends and holidays of the region
        :return: list of datetime values
        """
        working_days = []
        lux_hols = holidays.Luxembourg()[startDate:endDate]
        for i in range(int((endDate - startDate).days)):
            next_date = startDate + datetime.timedelta(i)
            if next_date.weekday() not in (5, 6) and next_date not in lux_hols:
                working_days.append(next_date)

        return working_days

    def missingDataPerInstrument(self):
        """
        Prints percentage of data missing in each columns
        :return:
        """
        instruments = self.df.groupby(['Subfund_Code', 'Share_Class_Code'])

        for name, group in instruments:
            print("\nInstrument - (Subbfund, Share_Class) : ", name)

            for c in group.columns:
                mispercent = (1 - (group[c].count() / group.shape[0])) * 100
                if mispercent > 0:
                    print("Column '{col}' has {x}% data missing".format(col=c, x=mispercent))

    def handleSerialDates(self):
        """
        Date columns of type float are of the excel serial data format http://www.cpearson.com/excel/datetime.htm
        This function converts its to date format
        :return:
        """
        for c in self.df.columns:
            if "date" in c.lower() and self.df[c].dtype in ['float64', 'float32']:
                date_series = self.df[c]
                seconds = [(d - 25569) * 86400.0 if d > 0 else None for d in date_series]
                date_series = [datetime.datetime.fromtimestamp(s).date() if s is not None else None for s in seconds]
                self.df[c] = date_series
                print("Updated column ", c)

    def findInstrumentCorrelation(self):

        # Fix missing Valuation Dates by adding the missing rows
        self.df['Valuation_Date'] = pd.to_datetime(self.df.Valuation_Date)
        start_dt = min(self.df.Valuation_Date)
        end_dt = max(self.df.Valuation_Date)

        full_df = pd.DataFrame({'Valuation_Date': self.dates_generator(start_dt, end_dt)})

        instruments = self.df.groupby(['Subfund_Code', 'Share_Class_Code'])
        for name, group in instruments:
            nav_df = pd.DataFrame(
                {'Valuation_Date': group.Valuation_Date, "{i}_{j}".format(i=name[0], j=name[1]): group.NAV_Per_Share})
            full_df = pd.merge(full_df, nav_df, how="left")

        corr_mat = full_df.corr(method='pearson', min_periods=int(full_df.shape[0] / 2))

        return corr_mat


if __name__ == "__main __":
    pass
