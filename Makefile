docker_image := iomete/tpcds-iceberg-generator
docker_tag := 1.0.0

build:
	mvn clean package

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
