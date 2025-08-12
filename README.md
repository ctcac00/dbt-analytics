# Jaffle Shop dbt Project

This is a dbt project for the Jaffle Shop, a fictional ecommerce store.

## Introduction

This project demonstrates a simple dbt workflow for transforming data from a transactional database into a format suitable for analytics. The project uses dbt to build models that represent customers and orders, and then joins them to create a denormalized table that is easy to query.

## Project Structure

The project is organized as follows:

- `dbt_project.yml`: The main configuration file for the dbt project.
- `models/`: Contains the dbt models.
  - `staging/`: Contains the staging models, which are responsible for cleaning and preparing the raw data.
  - `marts/`: Contains the marts models, which are responsible for creating the final tables that are used for analytics.
- `seeds/`: Contains the seed files, which are used to load data into the database.
- `tests/`: Contains the tests for the dbt models.

## Models

The project contains the following models:

- `stg_jaffle_shop__customers`: Cleans and prepares the raw customer data.
- `stg_jaffle_shop__orders`: Cleans and prepares the raw order data.
- `stg_stripe__payments`: Cleans and prepares the raw payment data.
- `dim_customers`: Creates a dimension table for customers.
- `fct_orders`: Creates a fact table for orders.

## Running the project

To run the project, you will need to have dbt installed. You can then run the following commands:

```
dbt run
```

This will run all of the models in the project.

## Tests

The project contains tests for the dbt models. To run the tests, you can run the following command:

```
dbt test
```

## Dependencies

This project requires the following dependencies:

- dbt
- postgres
