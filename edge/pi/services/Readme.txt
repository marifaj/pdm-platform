MVA services 

Start one service:
cd ~/mva 
./services.sh start-one ingestion 
cd ~/mva 
./services.sh start-one storage 
cd ~/mva 
./services.sh start-one inference 
cd ~/mva 
./services.sh start-one event_processing 
cd ~/mva 
./services.sh start-one notification

start all: 
cd ~/mva ./services.sh start


stop one:
cd ~/mva ./services.sh stop-one ingestion

stop all:
cd ~/mva ./services.sh stop

check status:
cd ~/mva ./services.sh status


CHECK HEALTH 
cd ~/mva 
./services.sh health

check performance usage of services:
ps -eo pid,%cpu,%mem,cmd | grep -E "ingestion|storage|inference|event_processing|notification" | grep -v grep
