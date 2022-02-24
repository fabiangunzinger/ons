"""
Create lookup table.

"""

import os
import ons.config as cf
import ons.helpers as hr


var_adders = []


def var_adder(func):
    var_adders.append(func)
    return func


def read_base(**kwargs):
    """Returns NSPL base file.

    File contains, for each postcode sector, codes for all required variabels
    (to be linked below).

    UK postcode format is as follows: SW1A 2AA, where left bit is outcode and
    right bit is incode. Sector is outcode + first element of incode, so 'SW1A
    2' above.
    """
    columns = {
        "pcds": "postcode",
        "doterm": "termination_date",
        "rgn": "region_code",
        "ru11ind": "rural_urban_code",
    }
    fp = os.path.join(cf.AWS_BUCKET, "raw/Data/NSPL_AUG_2020_UK.csv")
    df = hr.read_csv(fp, usecols=columns, dtype={"ru11ind": "object"}, **kwargs).rename(
        columns=columns
    )

    # keep active postcodes only
    # active postcodes are those without termination date
    df = df[df.termination_date.isna()].drop(columns="termination_date")

    # create postcode sector
    # keep first occurring region code for each postcode sector
    df["pcsector"] = df.postcode.str[:-2]
    df = df.drop_duplicates(subset=["pcsector"], keep="first")
    df = df.drop(columns="postcode")

    return df


@var_adder
def read_region():
    """Returns table with region names for each region code."""
    columns = {
        "GOR10CD": "region_code",
        "GOR10NM": "region_name",
    }
    bucket = cf.AWS_BUCKET
    path = "raw/Documents"
    file = "Region names and codes EN as at 12_10 (GOR).csv"
    fp = os.path.join(bucket, path, file)
    df = hr.read_csv(fp, usecols=columns).rename(columns=columns)

    # remove pseudo region code indicators
    # (e.g. '(pseudo) Wales' -> 'Wales')
    df["region_name"] = df.region_name.str.replace(
        r"\(pseudo\)", "", regex=True
    ).str.strip()

    return df


@var_adder
def read_rural_urban():
    columns = {
        "RU11IND": "rural_urban_code",
        "RU11NM": "rural_urban_name",
    }
    bucket = cf.AWS_BUCKET
    path = "raw/Documents"
    file = "Rural Urban (2011) Indicator names and codes GB as at 12_16.csv"
    fp = os.path.join(bucket, path, file)
    df = hr.read_csv(fp).rename(columns=columns)

    # remove country prefix
    df["rural_urban_name"] = df.rural_urban_name.str.replace(
        r"\([\w/]*\)", "", regex=True,
    ).str.strip()

    return df


def main(**kwargs):
    """Merge all columns into base file."""
    base = read_base(**kwargs)
    for var_adder in var_adders:
        base = base.merge(var_adder(), how="left", validate="m:1")
    base = base.sort_index(axis=1)

    bucket = cf.AWS_BUCKET
    path = 'clean'
    file = 'lookup.csv'
    fp = os.path.join(bucket, path, file)
    hr.write_csv(base, fp, verbose=True)
    return base


if __name__ == "__main__":
    main()
    # filename = "region_lookup_table.parquet"
    # filepath = os.path.join(config.AWS_BUCKET, filename)
    # try:
    #     regions = hr.read_parquet(filepath)
    # except FileNotFoundError:
    #     regions = _make_region_lookup_table()
    # regions = regions.rename(columns={"pcsector": "postcode"})
    # df = df.merge(regions, how="left", on="postcode", validate="m:1")
