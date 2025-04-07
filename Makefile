docker_image := iomete.azurecr.io/iomete/tpcds-iceberg-generator
docker_tag := 1.3.0

build:
	mvn clean package

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --push --platform linux/amd64,linux/arm64 --sbom=true --provenance=true -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
