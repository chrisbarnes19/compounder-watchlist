import numpy as np
import pandas as pd
from sec_edgar_downloader import Downloader
from bs4 import BeautifulSoup
import glob
import csv
import itertools
import os

# dictionary with fund names and CIKs

compounder_funds = {
    "AKO Capital": 1376879,
    "Akre": 1112520,
    "Altarock": 1631014,
    "Baillie Gifford": 1088875,
    "BLS Capital": 1670104,
    "Brown Capital": 885062,
    "Cantillon": 1279936,
    "Ensemble": 1387366,
    "Fundsmith": 1569205,
    "Giverney": 1641864,
    "Mar Vista": 1419999,
    "Markel": 1096343,
    "Meritage": 1427119,
    "Ruane Cunniff": 1720792,
    "Russo": 860643,
    "Stockbridge": 1505183,
    "TCI": 1647251,
    "Tiger Global": 1167483,
    "Triple Frond": 1454502,
    "WCM": 1061186
}

def download_filings(funds):
    """
    Takes a dictionary of funds with CIKs and downloads the most recent 13F
    """
    dl = Downloader()

    for fund in funds:
        dl.get("13F-HR",str(funds[fund]),amount=1)

def create_filing_df(funds):
    """
    Takes a list of funds and creates a dataframe with the downloaded filings
    """
    holdings = []

    for fund in funds:

        padded_CIK = str(funds[fund]).zfill(10)

        path = f"./sec-edgar-filings/{padded_CIK}/13F-HR/*/full-submission.txt"

        with open(glob.glob(path)[0]) as f:
            contents = f.read()

        soup = BeautifulSoup(contents,'xml')
        table = soup.find("informationTable")
        df = pd.read_xml(table.prettify())

        holdings.append(df)

    return holdings


def generate_watchlist(holdings):

    unique_cusips = set()

    for filing in holdings:
        unique_cusips.update(filing['cusip'].tolist())


    counts = pd.DataFrame(list(unique_cusips), columns=['cusip'])
    counts['count'] = 0
    counts.set_index('cusip',inplace=True)

    for cusip in unique_cusips:
        for filing in holdings:
            if cusip in filing['cusip'].values:
                counts.at[cusip,'count'] += 1

    cusip_names = pd.read_csv('13flist2021q3.csv',names=['cusip','star','issuerName','issuerDescription','status'])
    cusip_names.set_index('cusip',inplace=True)
    counts = counts.merge(cusip_names,left_on='cusip',right_on='cusip')

    counts.sort_values('count',ascending=False,inplace=True)
    counts.drop(['star','status'], axis=1, inplace=True)

    unique_counts = list(set(counts['count']))[1:]

    output = []

    for num in unique_counts:

        if num == 2:

            # figure out how many columns to divide the 2s over since there are so many
            # first, find out how many names in the 2 column

            number_of_twos = len(counts[counts['count'] == 2]['issuerName'].tolist())

            # first, find out how many names are in the 3 column

            number_of_threes = len(counts[counts['count'] == 3]['issuerName'].tolist())

            # want the 2s col to be no longer than the 3s; this is basically math.ceil

            number_of_columns = number_of_twos // number_of_threes + (number_of_twos % number_of_threes > 0)

            for i in range(number_of_columns):

                names = [str(num)] + counts[counts['count'] == num]['issuerName'].tolist()[(i)*number_of_threes:(i+1)*number_of_threes]
                output.append(names)


        else:

            names = [str(num)] + counts[counts['count'] == num]['issuerName'].tolist()
            output.append(names)

        pivoted = list(map(list, itertools.zip_longest(*output, fillvalue=None)))


    with open('compounder_watchlist.csv','w',newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(pivoted)


def cleanup():

    dir_path = './sec-edgar-filings/'

    try:
        os.rmdir(dir_path)

    except OSError as e:
        print("Error cleaning up files.")


download_filings(compounder_funds)
holdings = create_filing_df(compounder_funds)
generate_watchlist(holdings)
cleanup()
print("Watchlist downloaded successfully!")
