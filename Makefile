docker_image := iomete/tpcds-iceberg-generator
docker_tag := 0.3.0

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
