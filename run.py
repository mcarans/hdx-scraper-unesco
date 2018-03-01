#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

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
        logger.info('Number of datasets to upload: %d' % len(countriesdata))

        for countrydata in countriesdata:
            dataset, showcase = generate_dataset_and_showcase(downloader, countrydata, endpoints_metadata)
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx(remove_additional_resources=True)
                resources = dataset.get_resources()
                resource_ids = [x['id'] for x in sorted(resources, key=lambda x: x['name'], reverse=True)]
                dataset.reorder_resources(resource_ids)
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, hdx_site='feature', user_agent_config_yaml=join(expanduser('~'), '.unescouseragent.yml'), project_config_yaml=join('config', 'project_configuration.yml'))

