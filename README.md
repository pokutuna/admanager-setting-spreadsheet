admanager-setting-spreadsheet
===

Create orders, lineitems, creatives from GoogleSpreadsheet to GoogleAdManager

## Preparation

- Create a GCP service account.
  - with GoogleSpreadsheet API
  - download JSON access key
- Share the spreadsheet to GCP service account
- Enable API access GoogleAdManager
- Add service account to GAM as a trafficker
- Write config

## Execute

`$ pipenv run python -m gasp.runner --config=config.json`
