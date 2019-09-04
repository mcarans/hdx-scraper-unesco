#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for scrapername.

'''
from os.path import join
from pprint import pprint

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.path import temp_dir
import hdx.utilities.downloader

from tests.testing_data import countrydata, dimensions, observations
from unesco import generate_dataset_and_showcase, get_countriesdata, get_endpoints_metadata


class TestUnesco:
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'arg', 'title': 'Argentina'}])  # add locations used in tests
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'sustainable development'}, {'name': 'demographics'}, {'name': 'socioeconomics'}, {'name': 'education'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

    @pytest.fixture(scope='function')
    def csv_content(self):
        r = hdx.utilities.downloader.Download(user_agent='test').download('https://raw.githubusercontent.com/mcarans/hdx-scraper-unesco/master/tests/fixtures/EDU_FINANCE.csv')
        content = r.content
        r.close()
        return content

    @pytest.fixture(scope='class')
    def endpoints_metadata(self):
        return {'EDU_FINANCE': ('Education: Financial resources',
                                'http://yyyy/data/UNESCO,EDU_FINANCE/..........%s.?',
                                'http://uis.unesco.org/en/topic/education-finance',
                                observations)}

    @pytest.fixture(scope='function')
    def downloader(self, csv_content):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://xxx/codelist/UNESCO/CL_AREA/latest?format=sdmx-json':
                    def fn():
                        return {'Codelist': [{'items': [countrydata]}]}
                    response.json = fn
                elif 'http://yyyy/data/UNESCO,EDU_FINANCE/' in url:
                    def fn():
                        return {'structure': {'name': 'Education: Financial resources',
                                              'dimensions': {'observation': observations}}}
                    response.json = fn
                    response.content = csv_content
                return response

            @staticmethod
            def get_full_url(url):
                url_prefix = 'http://yyyy/data/UNESCO,EDU_FINANCE/..........AR.?format=csv'
                if url[:len(url_prefix)] == url_prefix:
                    return '%s&locale=en&subscription-key=12345' % url

        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://xxx/', downloader)
        assert countriesdata == [countrydata]

    def test_get_endpoints_metadata(self, downloader, endpoints_metadata):
        endpoints = {'EDU_FINANCE': 'http://uis.unesco.org/en/topic/education-finance'}
        endpoints_metadata_actual = get_endpoints_metadata('http://yyyy/', downloader, endpoints)
        assert endpoints_metadata_actual == endpoints_metadata

    def test_generate_dataset_and_showcase(self, configuration, downloader, endpoints_metadata):
        with temp_dir('UNESCO') as folder:
            res = generate_dataset_and_showcase(downloader, countrydata, endpoints_metadata, folder=folder)
            dataset, showcase = next(res)
            assert dataset == {'tags': [{'name': 'sustainable development', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'socioeconomics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'education', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                               'owner_org': '18f2d467-dcf8-4b7e-bffa-b3c338ba3a7c', 'data_update_frequency': '365',
                               'title': 'UNESCO Education: Financial resources - Argentina',
                               'groups': [{'name': 'arg'}], 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                               'name': 'unesco-education-financial-resources-argentina', 'dataset_date': '01/01/1970-12/31/2014',
                               'subnational': '0'}
            resources = dataset.get_resources()

            assert resources == [{'description': 'Government expenditure per student', 'format': 'csv', 'name': 'XUNIT', 'resource_type': 'file.upload', 'url_type': 'upload'}]

            assert showcase == {'name': 'unesco-education-financial-resources-argentina-showcase',
                                'notes': 'Education, literacy and other indicators for Argentina',
                                'image_url': 'http://www.tellmaps.com/uis/internal/assets/uisheader-en.png',
                                'url': 'http://uis.unesco.org/en/country/AR',
                                'tags': [{'name': 'sustainable development',
                                          'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'demographics',
                                          'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'socioeconomics',
                                          'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'education', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                                'title': 'UNESCO Education: Financial resources - Argentina'}

