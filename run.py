#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

import time
from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from unesco import generate_dataset_and_showcase, get_countriesdata, get_endpoints_metadata

from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with Download(extra_params_yaml=join(expanduser("~"), 'unesco.yml')) as downloader:
        endpoints = Configuration.read()['endpoints']
        endpoints_metadata = get_endpoints_metadata(base_url, downloader, endpoints)
        countriesdata = get_countriesdata(base_url, downloader)
        no_calls = 4
        logger.info('Number of datasets to upload: %d' % len(countriesdata))

        for countrydata in countriesdata:
            dataset, showcase = generate_dataset_and_showcase(downloader, countrydata, endpoints_metadata)
            no_calls += 3
            if no_calls >= 96:
                time.sleep(3900)
                no_calls = 0
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, hdx_site='feature', project_config_yaml=join('config', 'project_configuration.yml'))

