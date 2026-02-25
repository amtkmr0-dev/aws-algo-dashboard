git fetch --all && git reset --hard origin/main
sudo docker build -t algo-dash .
sudo docker rm -f my-algo-dash
sudo docker run -d -p 8080:8000 --restart unless-stopped --name my-algo-dash algo-dash
