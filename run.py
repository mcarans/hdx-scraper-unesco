#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from unesco import generate_dataset_and_showcase, get_countriesdata, get_endpoints_metadata

from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-unesco'


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with temp_dir('UNESCO') as folder:
        with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup) as downloader:
            endpoints = Configuration.read()['endpoints']
            endpoints_metadata = get_endpoints_metadata(base_url, downloader, endpoints)
            countriesdata = get_countriesdata(base_url, downloader)

            logger.info('Number of datasets to upload: %d' % len(countriesdata))

            for countrydata in countriesdata:
                for dataset, showcase in generate_dataset_and_showcase(downloader, countrydata, endpoints_metadata, folder="UNESCO3", merge_resources=True, single_dataset=False): # TODO: fix folder
                    if dataset:
                        dataset.update_from_yaml()
                        dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                        resources = dataset.get_resources()
                        resource_ids = [x['id'] for x in sorted(resources, key=lambda x: x['name'], reverse=False)]
                        dataset.reorder_resources(resource_ids, hxl_update=False)
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)

if __name__ == '__main__':
    facade(main, hdx_site='test', user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))

