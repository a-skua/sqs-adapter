init:
	docker compose up -d sqs
	sleep 3
	docker compose run --rm aws sqs create-queue --endpoint-url http://sqs:9324 --queue-name queue

run:
	docker compose run --rm worker

send:
	docker compose run --rm aws sqs send-message --endpoint-url http://sqs:9324 --queue-url http://localhost:9324/000000000000/queue --message-body 'hello, world'
