# Deployment Instructions for Preprocessor

* Create the backup
* Remove bin, config, lib, folders under the existing preprocessor installation under f:\IBM\VAPIT\scripts\fileproc
* Unzip the distro
* Copy bin, config, lib to f:\IBM\VAPIT\scripts\fileproc
* Make sure that the PIT_ENV environment variable points to the right config file: Preprod: azure_preprod, Prod: azure_prod
* Verify user name and password for the database in the config file for the environment, update if needed