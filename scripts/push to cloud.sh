# Create ECR repositories
aws ecr create-repository --repository-name validate-data --region eu-west-1
aws ecr create-repository --repository-name transform-data --region eu-west-1
aws ecr create-repository --repository-name compute-kpis --region eu-west-1

# Login to ECR
aws ecr get-login-password --region eu-west-1 \
| docker login --username AWS --password-stdin 123456789.dkr.ecr.eu-west-1.amazonaws.com

# Validate-data
docker tag validate-data:latest 123456789.dkr.ecr.eu-west-1.amazonaws.com/validate-data:latest
docker push 123456789.dkr.ecr.eu-west-1.amazonaws.com/validate-data:latest

# Transform-data
docker tag transform-data:latest 123456789.dkr.ecr.eu-west-1.amazonaws.com/transform-data:latest
docker push 123456789.dkr.ecr.eu-west-1.amazonaws.com/transform-data:latest

# Compute-kpis
docker tag compute-kpis:latest 123456789.dkr.ecr.eu-west-1.amazonaws.com/compute-kpis:latest
docker push 123456789.dkr.ecr.eu-west-1.amazonaws.com/compute-kpis:latest

aws ecs create-cluster --cluster-name ecommerce-pipeline-cluster --region eu-west-1

aws logs create-log-group --log-group-name /ecs/validate-task --region eu-west-1
aws logs create-log-group --log-group-name /ecs/transform-task --region eu-west-1
aws logs create-log-group --log-group-name /ecs/compute-task --region eu-west-1


aws ecs register-task-definition --cli-input-json file://validate-task.json --region eu-west-1
aws ecs register-task-definition --cli-input-json file://transform-task.json --region eu-west-1
aws ecs register-task-definition --cli-input-json file://compute-task.json --region eu-west-1

aws ec2 describe-vpcs --region eu-west-1
aws ec2 describe-subnets --region eu-west-1



# subnet-Oc258e856fOe04aca = eu-west-1a
# subnet-Od8436345ee46b7b8 = eu-west-1b

aws ec2 create-security-group --group-name ecs-sg --description "ECS Security Group" --vpc-id vpc-0e735e22121505f8f --region eu-west-1
aws ec2 authorize-security-group-egress --group-id sg-0222dcff8c650af7c --protocol "-1" --port -1 --cidr 0.0.0.0/0 --region eu-west-1

# Transform (after validate succeeds)
aws ecs run-task --cluster ecommerce-pipeline-cluster --task-definition transform-task --launch-type FARGATE --network-configuration "awsvpcConfiguration={subnets=[subnet-0d8436345ee46b7b8,subnet-0c258e856f0e04aca ],securityGroups=[sg-0222dcff8c650af7c],assignPublicIp=ENABLED}" --region eu-west-1

# Compute (after transform succeeds)
aws ecs run-task --cluster ecommerce-pipeline-cluster --task-definition compute-task --launch-type FARGATE --network-configuration "awsvpcConfiguration={subnets=[subnet-0d8436345ee46b7b8,subnet-0c258e856f0e04aca ],securityGroups=[sg-0222dcff8c650af7c],assignPublicIp=ENABLED}" --region eu-west-1

# Validate
aws ecs run-task \
    --cluster ecommerce-pipeline-cluster \
    --task-definition validate-task \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-0d8436345ee46b7b8,subnet-0c258e856f0e04aca],securityGroups=[sg-0222dcff8c650af7c],assignPublicIp=ENABLED}" \
    --region eu-west-1 \
    --overrides '{
        "containerOverrides": [
            {
                "name": "validate-container",
                "command": [
                    "s3://real-time-bucket-p4-zuki/temp/merge/order_items.csv"
                ]
            }
        ]
    }'


# Transform
aws ecs run-task \
    --cluster ecommerce-pipeline-cluster \
    --task-definition transform-task \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-0d8436345ee46b7b8,subnet-0c258e856f0e04aca],securityGroups=[sg-0222dcff8c650af7c],assignPublicIp=ENABLED}" \
    --region eu-west-1 \
    --overrides '{
        "containerOverrides": [
            {
                "name": "transform-container",
                "command": [
                    "s3://real-time-bucket-p4-zuki/temp/merge/order_items.csv",
                    "s3://real-time-bucket-p4-zuki/data/products.csv",
                    "s3://real-time-bucket-p4-zuki/temp/transforms/order_items_transformed.csv"
                ]
            }
        ]
    }'


aws ecs run-task \
    --cluster ecommerce-pipeline-cluster \
    --task-definition transform-task \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-0d8436345ee46b7b8,subnet-0c258e856f0e04aca],securityGroups=[sg-0222dcff8c650af7c],assignPublicIp=ENABLED}" \
    --region eu-west-1 \
    --overrides '{
        "containerOverrides": [
            {
                "name": "transform-container",
                "command": [
                    "s3://real-time-bucket-p4-zuki/temp/merge/orders.csv",
                    "none",
                    "s3://real-time-bucket-p4-zuki/temp/transforms/orders_transformed.csv"
                ]
            }
        ]
    }'


aws ecs run-task \
    --cluster ecommerce-pipeline-cluster \
    --task-definition transform-task \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-0d8436345ee46b7b8,subnet-0c258e856f0e04aca],securityGroups=[sg-0222dcff8c650af7c],assignPublicIp=ENABLED}" \
    --region eu-west-1 \
    --overrides '{
        "containerOverrides": [
            {
                "name": "transform-container",
                "command": [
                    "s3://real-time-bucket-p4-zuki/data/products.csv",
                    "none",
                    "s3://real-time-bucket-p4-zuki/data/temp/transforms/products_transformed.csv"
                ]
            }
        ]
    }'


# compute
aws ecs run-task \
    --cluster ecommerce-pipeline-cluster \
    --task-definition compute-task \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-0d8436345ee46b7b8,subnet-0c258e856f0e04aca],securityGroups=[sg-0222dcff8c650af7c],assignPublicIp=ENABLED}" \
    --region eu-west-1 \
    --overrides '{
        "containerOverrides": [
            {
                "name": "compute-container",
                "command": [
                    "s3://real-time-bucket-p4-zuki/temp/transforms/order_items_transformed.csv",
                    "s3://real-time-bucket-p4-zuki/temp/transforms/orders_transformed.csv",
                    "s3://real-time-bucket-p4-zuki/output/kpis/category_kpis.csv",
                    "s3://real-time-bucket-p4-zuki/output/kpis/order_kpis.csv"
                ]
            }
        ]
    }'

