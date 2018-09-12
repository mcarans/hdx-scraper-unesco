### Collector for UNESCO's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdxscraper-unesco.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdxscraper-unesco) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdxscraper-unesco/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdxscraper-unesco?branch=master)

This collector connects to the [UNESCO API](https://apiportal.uis.unesco.org/) and extracts data for 5 endpoints (DEM_ECO, EDU_FINANCE, EDU_NON_FINANCE, EDU_REGIONAL_MODULE, SDG4) country by country creating a dataset per country in HDX. Due to the UNESCO API having a very small quota limit (100 calls per hour), the scraper periodically hits this limit and must then poll every minute until the quota is renewed. Hence it can take around half a day to complete. It makes in the order of 1000 reads from UNESCO and 1000 read/writes (API calls) to HDX in total. It does not create temporary files as it puts urls into HDX. It is run when UNESCO make changes (not in their data but for example in their endpoints or API), in practice this is in the order of once or twice a year.  

### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdxscraper-fts** as specified in the parameter *user_agent_lookup*.
