git pull

sc stop SN-Server
sc stop SN-Tasks
sc stop SN-Integrator
sc stop SN-Inventory
sc stop SN-Consumers

TIMEOUT /t 3
sc start SN-Server
sc start SN-Consumers
sc start SN-Tasks
sc start SN-Integrator
sc start SN-Inventory
