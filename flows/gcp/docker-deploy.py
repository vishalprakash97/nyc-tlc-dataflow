from parameterized_web_to_gcs import etl_parent_flow
from prefect.infrastructure.container import DockerContainer
from prefect.deployments import Deployment

docker_block = DockerContainer.load("de-camp")
docker_dep=Deployment.build_from_flow(flow=etl_parent_flow,name='docker-flow',
                                      infrastructure=docker_block, entrypoint='flows/parameterized_web_to_gcs.py:etl_parent_flow')


if __name__ == "__main__":
    docker_dep.apply()
