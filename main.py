from InstrumentDataAnalysis import *
from firebase_connect import *

if __name__ == "__main __":

    ngt_fund_data = InvFundData("data/ngt_software_engineer_test_example_data (clean columns).csv")

    print("Perform Data Checks ...")

    num_data_points, mean_NAV = ngt_fund_data.df_facts()
    ngt_fund_data.missingDataPerInstrument()
    val_data_miss = ngt_fund_data.missingInstrumentValuations()
    nav_ccy_check = ngt_fund_data.consistent_CCY('CCY_NAV_share')

    print("Handle Series type dates if any")
    ngt_fund_data.handleSerialDates()

    # Calculating correlation between instruments
    instrument_correlation = ngt_fund_data.findInstrumentCorrelation()

    connect2firebase()
    writeDF('instrument_correlations', instrument_correlation)

    dq_ref = db.reference('ngt-inv-corr/data_quality_checks')
    for k in list(num_data_points.keys()):
        ins_ref = dq_ref.child('{i}_{j}'.format(i=k[0], j=k[1]))
        ins_ref.set({
            'Nb_data_points': int(num_data_points[k]),
            'Nb_Val_Days_Missing': int(val_data_miss[k]),
            'Mean_NAV': int(mean_NAV[k]),
            'NAV_CCY': ','.join(nav_ccy_check[k])
        })
