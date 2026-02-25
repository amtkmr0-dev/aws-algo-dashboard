git fetch --all && git reset --hard origin/main
docker compose down || true
docker rm -f my-algo-dash || true
docker build -t algo-dash .
docker run -d -p 8080:8000 --restart unless-stopped --name my-algo-dash algo-dash
