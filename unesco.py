#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
UNESCO:
------

Reads UNESCO API and creates datasets.

"""

import logging

import time

import sys
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.downloader import DownloadError
from six import reraise
from slugify import slugify
from io import BytesIO
import pandas as pd
import numpy as np
from os.path import join

logger = logging.getLogger(__name__)

MAX_OBSERVATIONS = 29990
dataurl_suffix = 'format=sdmx-json&detail=structureonly&includeMetrics=true'


def get_countriesdata(base_url, downloader):
    response = downloader.download('%scodelist/UNESCO/CL_AREA/latest?format=sdmx-json' % base_url)
    jsonresponse = response.json()
    return jsonresponse['Codelist'][0]['items']


def get_endpoints_metadata(base_url, downloader, endpoints):
    endpoints_metadata = dict()
    for endpoint in sorted(endpoints):
        base_dataurl = '%sdata/UNESCO,%s/' % (base_url, endpoint)
        datastructure_url = '%s?%s' % (base_dataurl, dataurl_suffix)
        response = downloader.download(datastructure_url)
#        open("endpointmeta_%s.json"%endpoint,"wb").write(response.content)  #TODO Clean
#        print("META "+endpoint)
        json = response.json()
        indicator = json['structure']['name']
        dimensions = json['structure']['dimensions']['observation']
        urllist = [base_dataurl]
        for dimension in dimensions:
            if dimension['id'] == 'REF_AREA':
                urllist.append('%s')
            else:
                urllist.append('.')
        urllist.append('?')
        structure_url = ''.join(urllist)
        endpoints_metadata[endpoint] = indicator, structure_url, endpoints[endpoint], dimensions
#    with open("endpoints_metadata.json","w") as f: #TODO Clean
#        import json
#        f.write(json.dumps(endpoints_metadata))
    return endpoints_metadata

def expand_column_labels(df):
    column_expansion_table = {row.split()[0]:" ".join(row.split()[1:]) for row in """
AGE                Age
COUNTRY_ORIGIN     Country / region of origin
REGION_DEST        Destination region
EDU_FIELD          Field of education
FUND_FLOW          Funding flow
GRADE              Grade
IMM_STATUS         Immigration status
INFRASTR           Infrastructure
EDU_LEVEL          Level of education
EDU_ATTAIN         Level of educational attainment
LOCATION           Location
REF_AREA           Reference area
SUBJECT            Subject
SEX                Sex
SE_BKGRD           Socioeconomic background
STAT_UNIT          Statistical unit
TEACH_EXPERIENCE   Teaching experience
TIME_PERIOD        Time Period
CONTRACT_TYPE      Type of contract
EDU_TYPE           Type of education
EXPENDITURE_TYPE   Type of expenditure
UNIT_MEASURE       Unit of measure
WEALTH_QUINTILE    Wealth quintile
    """.split("\n") if len(row.split())>=2}
    def expand_label(label):
        if label in column_expansion_table:
            return column_expansion_table[label]
        else:
            label = label.lower().replace("_"," ")
            label = label[0].upper()+label[1:]
            return label
    return df.rename(columns={c:expand_label(c) for c in df.columns})
    

def split_columns_df(df, code_column_postfix = " code", store_code = False):
    split_columns = [x.strip() for x in """    
Age
AGE
Country / region of origin
COUNTRY_ORIGIN
Destination region
REGION_DEST
Field of education
EDU_FIELD
Funding flow
FUND_FLOW
Grade
GRADE
Immigration status
IMM_STATUS
Infrastructure
INFRASTR
Level of education
EDU_LEVEL
Level of educational attainment
EDU_ATTAIN
Location
LOCATION
Orientation
Reference area
REF_AREA
School subject
SUBJECT
Sex
SEX
Socioeconomic background
SE_BKGRD
Source of funding
Statistical unit
STAT_UNIT
Teaching experience
TEACH_EXPERIENCE
Time Period
TIME_PERIOD
Type of contract
CONTRACT_TYPE
Type of education
EDU_TYPE
Type of expenditure
EXPENDITURE_TYPE
Type of institution
Unit of measure
UNIT_MEASURE
Wealth quintile
WEALTH_QUINTILE
""".split("\n") if len(x) and x in df.columns]
    new_df=pd.DataFrame()
    for c in split_columns:
#        values = [":".join(x.split(":")[1:]).replace("Not applicable","NA") for x in df[c].values]
        def cleanval(x):
            if isinstance(x,str):
                if ":" in x:
                    return ":".join(x.split(":")[1:])
            return x
        values = [cleanval(x) for x in df[c].values]
        new_df[c]=values
        if store_code:
            cc = c + code_column_postfix
            codes = [x.split(":")[0] for x in df[c].values]
            new_df[cc]=codes
    for c in [x for x in df.columns if x not in split_columns]:
        new_df[c]=df[c].values
    return new_df

def expand_time_columns_df(df, time_column="TIME_PERIOD", value_column="OBS_VALUE"):
    year_columns = [y for y in df.columns if str(y).isdigit()]
    copy_columns = [c for c in df.columns if c not in year_columns]
    new_df=pd.DataFrame(columns = copy_columns+[time_column, value_column])
    dfc = df[copy_columns]
    for y in year_columns:
        dfblock = dfc.copy()
        dfblock[time_column] = y
        dfblock[value_column] = df[y].values
        new_df = new_df.append(dfblock, ignore_index=True)
    return new_df

def add_hxl_tags(df, time_column = "TIME_PERIOD", value_column = "OBS_VALUE", code_column_postfix = " code"):
    """Add the HXL tags to dataframe.
    """
    column_definition="""    
Age                                        #group+age
Country / region of origin                 #country+origin
Destination region                         #region+destination
Field of education                         #indicator+education+field+name
Funding flow                               #indicator+funding+flow+name
Grade                                      #indicator+grade
Immigration status                         #indicator+immigration+status
Infrastructure                             #indicator+infrastructure
Level of education                         #group+education+level
Level of educational attainment            #group+education+level+attainment
Location                                   #geo+location+type
Orientation                                #indicator+orientation
Reference area                             #geo+reference+area
School subject                             #indicator+school+subject+name
Sex                                        #group+sex
Socioeconomic background                   #group+socioeconomic+background
Source of funding                          #indicator+funding+source
Statistical unit                           #indicator+statistical+unit
Teaching experience                        #indicator+teaching+experience
Time Period                                #date
Type of contract                           #indicator+contract+name
Type of education                          #indicator+education+type+name
Type of expenditure                        #indicator+expenditure+type+name
Type of institution                        #indicator+institution+type+name
Unit of measure                            #meta+unit+measure+name
Wealth quintile                            #indicator+wealth+quintile+name

AGE                                        #group+age
BASIC_SERVICES                             #indicator+basic+services
CONTRACT_TYPE                              #indicator+contract+name
CLASS_TYPE                                 #indicator+class+name
COUNTRY_ORIGIN                             #country+origin
EDU_ATTAIN                                 #group+education+level+attainment
EDU_CAT                                    #indicator+education+category+name
EDU_FIELD                                  #indicator+education+field+name
EDU_LEVEL                                  #group+education+level
EDU_TYPE                                   #indicator+education+type+name
FUND_FLOW                                  #indicator+funding+flow+name
GRADE                                      #indicator+grade
IMM_STATUS                                 #indicator+immigration+status
INFRASTR                                   #indicator+infrastructure
FREQ                                       #indicator+frequency
LOCATION                                   #geo+location+type
REF_AREA                                   #geo+reference+area
REGION_DEST                                #region+destination
SUBJECT                                    #indicator+school+subject+name
SECTOR_EDU                                 #indicator+sector+name
SEX                                        #group+sex
SE_BKGRD                                   #group+socioeconomic+background
SOURCE_FUND                                #indicator+funding+source
STAT_UNIT                                  #indicator+statistical+unit
TEACH_EXPERIENCE                           #indicator+teaching+experience
TIME_PERIOD                                #date
EXPENDITURE_TYPE                           #indicator+expenditure+type+name
UNIT_MEASURE                               #meta+unit+measure+name
UNIT_MULT                                  #meta+unit+mult+name
DECIMALS                                   #meta+decimals
WEALTH_QUINTILE                            #indicator+wealth+quintile+name
DISPERSION
OBS_STATUS
TEXTB_TYPE
    """.split('\n')

    hxl={time_column : "#date", value_column : "#indicator+value+num"}
    for x in column_definition:
        v=x.split("#")
        if len(v)!=2:
            continue
        column_name = v[0].strip()
        column_hxl = "#"+(" ".join(v[1:])) if len(v)>1 else ""
        if column_name in df.columns:
            hxl[column_name]=column_hxl
        column_name += code_column_postfix
        column_hxl += "+code" if len(column_hxl)>1 else ""
        if column_name in df.columns:
            hxl[column_name]=column_hxl

    return pd.DataFrame(data=[hxl],columns=df.columns).append(df,ignore_index=True)

def process_df(df, code_column_postfix = " code", store_code = False, time_column = "TIME_PERIOD", value_column = "OBS_VALUE"):
    """
    Processed the raw (merged) data into a desired format:
    Code (id) is removed from string values and optionally (if store_code is True) saved in "code" columns (with column name postfixed by code_column_postfix).
    All time-period columns are put into separate rows, original period is stored in time_column, value in value_column.
    Rows without values are removed.
    HXL tags are added.
    :param df: DataFrame with input data
    :param code_column_postfix: postfix fo code columns (used only if store_code is True)
    :param store_code: contrrolls whether code part of string values is stored
    :param time_column: name of a column to store the year
    :param value_column: name of the column to store the values
    :return: resulting DataFrame
    """
    #df = df.drop(columns="TIME_PERIOD") # Drop this columns because it is redundant - codes are present in string values
    df = split_columns_df(df, code_column_postfix = code_column_postfix, store_code = store_code)
    #df = expand_time_columns_df(df, time_column = time_column, value_column = value_column)

    # Remove rows lacking a value
    index = ~(df[value_column].isna() | np.array([len(str(x).strip())==0 for x in df[value_column]]))
    df1 = df.loc[index].sort_values(by=[time_column]) # select and sort

    df2 = add_hxl_tags(df1, time_column = time_column, value_column = value_column, code_column_postfix = code_column_postfix)
    return df2

def postprocess_df(df):
    "Do final adjustments to the dataframe before publishing."
    return expand_column_labels(df)

def split_df_by_column(df, column):
    if column is None:
        yield None, df
    else:
        tags = df.iloc[[0], :]
        data = df.iloc[1:, :]
        other_columns = [c for c in df.columns if c!=column]
        for x in (sorted(data[column].unique())):
            df_part = tags[other_columns].append(data.loc[data[column]==x,other_columns], ignore_index=True)
            yield x, df_part

def remove_useless_columns_from_df(df):
    data = df.iloc[1:, :]
    for c in df.columns:
        values = data[c].unique()
        if len(values)==1:
            if isinstance(values[0], str):
                if values[0].lower() in ["total", "_t", "not applicable", "_z", "na"] or str(values[0]).lower().startswith("all "):
                    df=df.drop(columns=c)
    return df


def create_dataset_showcase(name, countryname, countryiso2, countryiso3, single_dataset=False):
    slugified_name = slugify(name).lower()
    slugified_name = slugified_name.replace("united-kingdom-of-great-britain-and-northern-ireland","uk") # Too long
    slugified_name = slugified_name.replace("demographic-and-socio-economic-indicators","dsei") # Too long
    if single_dataset:
        title = '%s - Sustainable development, Education, Demographic and Socioeconomic Indicators' % countryname
    else:
        title = name
    dataset = Dataset({
        'name': slugified_name,
        'title': title
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('18f2d467-dcf8-4b7e-bffa-b3c338ba3a7c')
    dataset.set_subnational(False)
    try:
        dataset.add_country_location(countryiso3)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None,None
    dataset.set_expected_update_frequency('Every year')
    tags = ['sustainable development', 'demographics', 'socioeconomics', 'education']
    dataset.add_tags(tags)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': name,
        'notes': 'Education, literacy and other indicators for %s' % countryname,
        'url': 'http://uis.unesco.org/en/country/%s' % countryiso2,
        'image_url': 'http://www.tellmaps.com/uis/internal/assets/uisheader-en.png'
    })
    showcase.add_tags(tags)

    return dataset, showcase

def load_safely(downloader, url):
    """
    Safely load data from URL - wait if quota is exceeded
    :param downloader: Downloader object
    :param url: url to fetch
    :return: response object
    """
    response = None
    while response is None:
        try:
            response = downloader.download(url)
        except DownloadError:
            exc_info = sys.exc_info()
            tp, val, tb = exc_info
            if 'Quota Exceeded' in str(val.__cause__):
                logger.info('Sleeping for one minute')
                time.sleep(60)
            elif 'Not Found' in str(val.__cause__):
                logger.exception("Resource not found: %s"%url)
                return None
            else:
                logger.exception("UNFORSEEN ERROR: %s"%url)
                response = None
                #reraise(*exc_info)
    return response


def download_df(downloader, csv_url, start_year, end_year):
    """
    Download dataframe from csv_url with a specified period
    :param downloader: Downloader object
    :param csv_url: URL prefix to fetch data from
    :param start_year: start year of the period
    :param end_year: end year of the period
    :return: DataFrame or None in case of a failure
    """
    assert end_year >= start_year
    url_years = '&startPeriod=%d&endPeriod=%d' % (start_year, end_year)
    url = downloader.get_full_url('%s%s' % (csv_url, url_years))
    response = load_safely(downloader, url)
    if response is not None:
        return pd.read_csv(BytesIO(response.content), encoding="ISO-8859-1")

def chunk_years(time_periods, max_observations=None):
    """
    Chunk years to periods with a number of observations limited by max_observations.
    :param time_periods: dictionary of years -> number of observations
    :param max_observations: maximum numbver of observations (if None, default value is selected)
    :return: generator yielding pairs of (start year, end year)
    """
    if max_observations is None:
        max_observations=MAX_OBSERVATIONS

    years = np.sort(np.array(list(time_periods.keys())))[::-1]
    observation_per_year = np.array([time_periods.get(int(y),0) for y in years])

    while len(years):
        cumulative_sum = np.cumsum(observation_per_year)
        selection =  cumulative_sum <= max(max_observations, cumulative_sum[0])
        yield years[selection].min(),years[selection].max()
        years = years[~selection]
        observation_per_year=observation_per_year[~selection]

def generate_dataset_and_showcase(downloader,
                                  countrydata,
                                  endpoints_metadata,
                                  folder,
                                  merge_resources=True,
                                  single_dataset=False,
                                  split_to_resources_by_column = "STAT_UNIT",
                                  remove_useless_columns = True):
    """
    https://api.uis.unesco.org/sdmx/data/UNESCO,DEM_ECO/....AU.?format=csv-:-tab-true-y&locale=en&subscription-key=...

    :param downloader: Downloader object
    :param countrydata: Country datastructure from UNESCO API
    :param endpoints_metadata: Endpoint datastructure from UNESCO API
    :param folder: temporary folder
    :param merge_resources: if true, merge resources for all time periods
    :param single_dataset: if true, put all endpoints into a single dataset
    :param split_to_resources_by_column: split data into multiple resorces (csv) based on a value in the specified column
    :param remove_useless_columns:
    :return: generator yielding (dataset, showcase) tuples. It may yield None, None.
    """
    countryiso2 = countrydata['id']
    countryname = countrydata['names'][0]['value']
    logger.info("Processing %s"%countryname)

    if countryname[:4] in ['WB: ', 'SDG:', 'MDG:', 'UIS:', 'EFA:'] or countryname[:5] in ['GEMR:', 'AIMS:'] or \
            countryname[:7] in ['UNICEF:', 'UNESCO:']:
        logger.info('Ignoring %s!' % countryname)
        yield None, None
        return

    countryiso3 = Country.get_iso3_from_iso2(countryiso2)

    if countryiso3 is None:
        countryiso3, _ = Country.get_iso3_country_code_fuzzy(countryname)
        if countryiso3 is None:
            logger.exception('Cannot get iso3 code for %s!' % countryname)
            yield None, None
            return
        logger.info('Matched %s to %s!' % (countryname, countryiso3))

    earliest_year = 10000
    latest_year = 0


    if single_dataset:
        name = 'UNESCO indicators - %s' % countryname
        dataset, showcase = create_dataset_showcase(name, countryname, countryiso2, countryiso3, single_dataset=single_dataset)
        if dataset is None:
            return

    for endpoint in sorted(endpoints_metadata):
        time.sleep(0.2)
        indicator, structure_url, more_info_url, dimensions = endpoints_metadata[endpoint]
        structure_url = structure_url % countryiso2
        response = load_safely(downloader, '%s%s' % (structure_url, dataurl_suffix))
        json = response.json()
        if not single_dataset:
            name = 'UNESCO %s - %s' % (json["structure"]["name"], countryname)
            dataset, showcase = create_dataset_showcase(name, countryname, countryiso2, countryiso3, single_dataset=single_dataset)
            if dataset is None:
                continue
        observations = json['structure']['dimensions']['observation']
        time_periods = dict()
        for observation in observations:
            if observation['id'] == 'TIME_PERIOD':
                for value in observation['values']:
                    time_periods[int(value['id'])] = value['actualObs']
        if len(time_periods) == 0:
            logger.warning('No time periods for endpoint %s for country %s!' % (indicator, countryname))
            continue

        earliest_year = min(earliest_year, *time_periods.keys())
        latest_year = max(latest_year,*time_periods.keys())

        csv_url = '%sformat=csv' % structure_url

        description = more_info_url
        if description != ' ':
            description = '[Info on %s](%s)' % (indicator, description)
        description = 'To save, right click download button & click Save Link/Target As  \n%s' % description

        df = None
        for start_year, end_year in chunk_years(time_periods):
            if merge_resources:
                df1 = download_df(downloader, csv_url, start_year, end_year)
                if df1 is not None:
                    df = df1 if df is None else df.append(df1)
            else:
                url_years = '&startPeriod=%d&endPeriod=%d' % (start_year, end_year)
                resource = {
                    'name': '%s (%d-%d)' % (indicator, start_year, end_year),
                    'description': description,
                    'format': 'csv',
                    'url': downloader.get_full_url('%s%s' % (csv_url, url_years))
                }
                dataset.add_update_resource(resource)

        if df is not None:
            stat = {x["id"]: x["name"] for d in dimensions if d["id"] == "STAT_UNIT" for x in d["values"]}
            for value, df_part in split_df_by_column(process_df(df), split_to_resources_by_column):
                file_csv = join(folder,
                                ("UNESCO_%s_%s.csv" % (countryiso3, endpoint + ("" if value is None else "_"+value))
                                 ).replace(" ", "-").replace(":", "-").replace("/","-").replace(",","-")
                                .replace("(","-").replace(")","-"))
                if remove_useless_columns:
                    df_part = remove_useless_columns_from_df(df_part)
                df_part["country-iso3"]=countryiso3
                df_part.iloc[0,df_part.columns.get_loc("country-iso3")]="#country+iso3"
                df_part["Indicator name"]=value
                df_part.iloc[0,df_part.columns.get_loc("Indicator name")]="#indicator+name"
                df_part = postprocess_df(df_part)
                df_part.to_csv(file_csv, index=False)
                description_part = stat.get(value,'Info on %s%s' % ("" if value is None else value+" in ", indicator))
                resource = Resource({
                    'name': value,
                    'description': description_part
                })
                resource.set_file_type('csv')
                resource.set_file_to_upload(file_csv)
                dataset.add_update_resource(resource)

        if not single_dataset:
            if dataset is None or len(dataset.get_resources()) == 0:
                logger.error('No resources created for country %s, %s!' % (countryname, endpoint))
            else:
                dataset.set_dataset_year_range(min(time_periods.keys()),max(time_periods.keys()))
                yield dataset, showcase

    if single_dataset:
        if dataset is None or len(dataset.get_resources()) == 0:
            logger.error('No resources created for country %s!' % (countryname))
        else:
            dataset.set_dataset_year_range(earliest_year, latest_year)
            yield dataset, showcase
