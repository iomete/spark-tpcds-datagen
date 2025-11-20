docker_image := iomete.azurecr.io/iomete/tpcds-iceberg-generator
docker_tag := 3.5.5

build:
	./build/mvn clean package

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
