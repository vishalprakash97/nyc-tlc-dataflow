# Terraform for GCP
The repository includes Terraform code for deploying resources on Google Cloud.
### Initializing Variables

Create a .tfvars file containing the file path of the credentials and project name.

```tfvars
project = "<project-name"
region = "us-east1"
credentials = "<filepath>"
```

### Important Commands


1. terraform refresh
    - Get current state
2. terraform plan
    - Create an execution plan to get to the desired state 
    - No change to actual resources
3. terraform apply
    - Execute plan
4. terraform destroy
    - Destroy the resources/ infrastructure
