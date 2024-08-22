## Lambda Functions

### Commands
```shell
# For local testing
sam local invoke --event <json-file> --env-vars <json-file> <LambdaFunction>

# Example 
sam local invoke \
    --event events/clean_data.json \
    --env-vars .env/clean_data.json \
    CleanDataFunction
```

>1. Create json files for Events and environmnent variables required for testing
>2. The environment variables need to be in a json file. Refer to this [doc](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-using-invoke.html#serverless-sam-cli-using-invoke-environment-file)
for syntax.

