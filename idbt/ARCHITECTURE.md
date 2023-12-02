


Clasess:
- dbt project
    method
        - run project
        - run seed
        - compile each data source
        - get columns

    attributes
        [external]
        - diagram
            models
                - upstream
                - settings

        [internal]
        - yaml template
        - data sources
        - dbt models
        - dbt data sources

- dbt model
    attribute
        - template
        - upstream models / data sources
        - settings

    method
        - get columns
        - compile

- dbt data source
    method
        - get columns from csv
    attribute
        - filename