## Step Functions

### core_etl.asl.json

- Core ETL pipeline which calls the fetch,clean, and upload lambda functions in order
- Takes month and year as input

### monthly_flow.asl.json

- Fetches the month and year from SSM Parameter Store
- Runs the Core ETL pipeline with the month and year as input
- State machine is triggered on the 10th of every month to fetch new data
  - Schedule set up using EventBridge

### backfill_flow.asl.json

- Used to backfill data
- Runs the core ETL pipeline for a list of months
    ```json
    {
        "year":2024,
        "months":[1,2,3]
    } //sample input to state machine
    ```

- If you're using CLI,
    ```bash
    aws stepfunctions start-execution \
        --state-machine-arn <state-machine-arn> \
        --input '{ "year": 2024, "months": [1, 2, 3] }'
    ```