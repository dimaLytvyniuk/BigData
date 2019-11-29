docker kill $(docker ps -q)
docker rm $(docker ps -q -a)
docker rmi $(docker images -q)

docker volume rm $(docker volume ls -q)
