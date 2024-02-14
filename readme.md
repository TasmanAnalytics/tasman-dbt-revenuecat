[![tasman_logo][tasman_wordmark_black]][tasman_website_light_mode]
[![tasman_logo][tasman_wordmark_cream]][tasman_website_dark_mode]

---
*We are the boutique analytics consultancy that turns disorganised data into real business value. [Get in touch][tasman_contact] to learn more about how Tasman can help solve your organisations data challenges.*

# RevenueCat
This packages aims to transform the [RevenueCat](https://www.revenuecat.com/) data to cookiecutter datasets ready for analysis. This package does require to have the [RevenueCat scheduled data exports](https://www.revenuecat.com/docs/integrations/scheduled-data-exports) enabled and loaded into Snowflake.

## Configurations
To get started specify the location of the table in Snowflake by setting the variables `revenuecat_database`, `revenuecat_schema` and `revenuecat_table`. Optionally, it is possible to apply a filter by passing the `revenuecat_filter` variable. Common practice is to filter the sandbox transactions out of the analysis data. To parse the custom subscriber attributes specify the `revenuecat_custom_subscriber_attributes` dictionary with key, value pairs for the values to parse and the column names to provide it.

```
vars:
  tasman_dbt_revenuecat:
    revenuecat_database: "source_db"
    revenuecat_schema: 'revenuecat'
    revenuecat_table: "data_export"
    revenuecat_filter: "is_sandbox = false"
    revenuecat_custom_subscriber_attributes: {'my_value::text': 'my_column_name'}
```

## Supported Data Warehouses
This package currently only supports Snowflake.

## Contact
This package has been written and is maintained by [Tasman Analytics](https://tasman.ai).

If you find a bug, or for any questions please open an issue on GitHub.

[tasman_website_dark_mode]: https://tasman.ai?utm_source=github&utm_medium=internal-referral&utm_campaign=tasman-dbt-mta#gh-dark-mode-only
[tasman_website_light_mode]: https://tasman.ai?utm_source=github&utm_medium=internal-referral&utm_campaign=tasman-dbt-mta#gh-light-mode-only
[tasman_contact]: https://tasman.ai/contact?utm_source=github&utm_medium=internal-referral&utm_campaign=tasman-dbt-mta
[tasman_wordmark_cream]: https://raw.githubusercontent.com/TasmanAnalytics/.github/master/images/tasman_wordmark_cream_500.png#gh-dark-mode-only
[tasman_wordmark_black]: https://raw.githubusercontent.com/TasmanAnalytics/.github/master/images/tasman_wordmark_black_500.png#gh-light-mode-only