#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.data.dataset import Dataset
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
                resource_names = [x['name'] for x in dataset.get_resources()]
                dataset_name = dataset['name']
                dataset_del_res = Dataset.read_from_hdx(dataset_name)
                if dataset_del_res:
                    for resource in dataset_del_res.get_resources():
                        resource_name = resource['name']
                        if resource_name in resource_names:
                            continue
                        logger.info('Deleting resource: %s in dataset: %s' % (resource_name, dataset_name))
                        resource.delete_from_hdx()
                dataset.update_from_yaml()
                dataset.create_in_hdx()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, hdx_site='feature', user_agent_config_yaml=join(expanduser('~'), '.unescouseragent.yml'), project_config_yaml=join('config', 'project_configuration.yml'))

