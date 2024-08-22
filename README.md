# nyc-tlc-dataflow

Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Commands
```shell
# Validation
sam validate -t cloudformation/template.yaml --lint

# Build
sam build -t cloudformation/template.yaml

# Deployment
sam deploy --guided
```