docker_image := iomete/tpcds_iceberg_generator
docker_tag := 0.2.1

docker-build:
	# Run this for one time: docker buildx create --use
	docker build -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
