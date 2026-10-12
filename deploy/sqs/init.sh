#!/bin/bash
set -euo pipefail

EP=http://localstack:4566

create_fifo() {
  aws --endpoint-url "$EP" sqs create-queue \
    --queue-name "$1" \
    --attributes FifoQueue=true \
    || aws --endpoint-url "$EP" sqs get-queue-url --queue-name "$1" >/dev/null
}

create_fifo wager-transactions-dlq.fifo
create_fifo wager-events.fifo

aws --endpoint-url "$EP" sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes FifoQueue=true,VisibilityTimeout=30 \
  || aws --endpoint-url "$EP" sqs get-queue-url --queue-name wager-transactions.fifo >/dev/null

ARN=$(aws --endpoint-url "$EP" sqs get-queue-attributes \
  --queue-url "$EP/000000000000/wager-transactions-dlq.fifo" \
  --attribute-names QueueArn \
  --query Attributes.QueueArn \
  --output text)

raw="{\"deadLetterTargetArn\":\"${ARN}\",\"maxReceiveCount\":\"5\"}"
escaped=$(printf '%s' "$raw" | sed 's/\\/\\\\/g; s/"/\\"/g')

cat > /tmp/inbound.json <<EOF
{
  "VisibilityTimeout": "30",
  "RedrivePolicy": "${escaped}"
}
EOF

aws --endpoint-url "$EP" sqs set-queue-attributes \
  --queue-url "$EP/000000000000/wager-transactions.fifo" \
  --attributes file:///tmp/inbound.json
