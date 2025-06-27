[![tasman_logo][tasman_wordmark_black]][tasman_website_light_mode]
[![tasman_logo][tasman_wordmark_cream]][tasman_website_dark_mode]

---
*We are the boutique analytics consultancy that turns disorganised data into real business value. [Get in touch][tasman_contact] to learn more about how Tasman can help solve your organisations data challenges.*

# RevenueCat
This package aims to transform the [RevenueCat](https://www.revenuecat.com/) data into cookiecutter datasets ready for analysis. It requires that you have the [RevenueCat scheduled data exports](https://www.revenuecat.com/docs/integrations/scheduled-data-exports) enabled and loaded into your data warehouse (Snowflake or BigQuery).

**Key Features:**
- 🔥 Accurate deduplication of RevenueCat transaction data
- 🎉 Transformed core model set for transactions, activities, entitlements and products.
- 🏃 Out-of-the-box analysis models that align with RevenueCat's UI reporting logic.
- 🧩 Adapter-aware macros and models (supports Snowflake and BigQuery).

## Configurations

### Snowflake
To get started on Snowflake, specify the location of the table by setting the variables `revenuecat_database`, `revenuecat_schema` and `revenuecat_table`. Optionally, you can apply a filter (for example, to exclude sandbox transactions) by passing the `revenuecat_filter` variable. To parse custom subscriber attributes, specify the `revenuecat_custom_subscriber_attributes` dictionary with key–value pairs.

Example (Snowflake):

```yaml
vars:
  tasman_dbt_revenuecat:
    revenuecat_database: "source_db"
    revenuecat_schema: "revenuecat"
    revenuecat_table: "data_export"
    revenuecat_version: 5
    revenuecat_filter: "is_sandbox = false"
    revenuecat_custom_subscriber_attributes: {"my_value::text": "my_column_name"}
    revenuecat_mrr_test_seed: ""
```

### BigQuery
For BigQuery, the package uses adapter-aware macros (for example, for type casting, date diff, and surrogate key generation) so that the models work seamlessly. You can configure your BigQuery project (and optionally a partition field) as follows:

Example (BigQuery):

```yaml
vars:
  tasman_dbt_revenuecat:
    revenuecat_database: "{{ target.database }}"
    revenuecat_schema: "{{ target.schema }}"
    revenuecat_table: "transactions"
    revenuecat_version: 5
    revenuecat_filter: "is_sandbox = false"
    revenuecat_custom_subscriber_attributes: {'my_value': 'my_column_name'}
```

## Supported Data Warehouses
This package now supports both Snowflake and BigQuery.

## Contact
This package has been written and is maintained by [Tasman Analytics](https://tasman.ai).

If you find a bug or have any questions, please open an issue on GitHub.

[tasman_website_dark_mode]: https://tasman.ai?utm_source=github&utm_medium=internal-referral&utm_campaign=tasman-dbt-revenuecat#gh-dark-mode-only
[tasman_website_light_mode]: https://tasman.ai?utm_source=github&utm_medium=internal-referral&utm_campaign=tasman-dbt-revenuecat#gh-light-mode-only
[tasman_contact]: https://tasman.ai/contact?utm_source=github&utm_medium=internal-referral&utm_campaign=tasman-dbt-revenuecat
[tasman_wordmark_cream]: https://raw.githubusercontent.com/TasmanAnalytics/.github/master/images/tasman_wordmark_cream_500.png#gh-dark-mode-only
[tasman_wordmark_black]: https://raw.githubusercontent.com/TasmanAnalytics/.github/master/images/tasman_wordmark_black_500.png#gh-light-mode-only