docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group group-x
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group group-y
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --all-groups --describe | findstr "topic"
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group group-x --members --verbose