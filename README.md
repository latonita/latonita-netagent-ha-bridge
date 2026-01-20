# NetAgent HA Bridge

Polls a Megatec NetAgent UPS over HTTP, parses status pages, and
publishes live metrics + Home Assistant discovery data via MQTT. Includes
Docker and docker-compose definitions for easy deployment.

NetAgent cards support SNMP, however not all the data is exported, so this 
bridge was created.

## Quick start

```bash
cp .env.example .env
npm install
node index.js
```

Or run inside Docker:

```bash
docker compose up --build -d
```
