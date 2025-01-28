const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');

// Configuration object
const config = {
  kafka: {
    brokers: process.env.brokers,
    clientId: 'kafka-consumer-down',
    // Using a new consumer group to ensure we get all messages
    groupId: 'new-consumer-group-' + Date.now(),
  },
  topic: 'flink-test-1',
  filter: {
    keyword: 'OutletDetails',
    timeRange: {
      start: new Date(Date.UTC(new Date().getFullYear(), new Date().getMonth(), new Date().getDate()-1, 0, 0, 0)).getTime(), // One day prior at 12:00:00
      end: new Date(Date.UTC(new Date().getFullYear(), new Date().getMonth(), new Date().getDate(), 0, 0, 0)).getTime()
    }
  },
  outputFile: path.join(__dirname, 'kafka_messages.json'),
  performance: {
    batchSize: 1000,
    flushInterval: 5000,
    progressInterval: 10000,
    bufferSize: 64 * 1024,
    noActivityTimeout: 15000 //milli seconds timeout for no new records
  }
};

async function consumeMessages() {
  console.log('Starting Kafka consumer with config:', JSON.stringify(config, null, 2));
  console.log(config.filter.timeRange.start);
  console.log(config.filter.timeRange.end);
  const kafka = new Kafka({
    clientId: config.kafka.clientId,
    brokers: config.kafka.brokers.split(','),
  });

  // Create a new consumer
  const consumer = kafka.consumer({
    groupId: config.kafka.groupId,
    readUncommitted: true,
  });

  console.log('Connecting to Kafka...');
  await consumer.connect();
  console.log('Connected successfully');

  // Subscribe to topic from specified timestamp
  console.log(`Subscribing to topic: ${config.topic}`);
  await consumer.subscribe({
    topic: config.topic,
    fromBeginning: true
  });

  const outputStream = fs.createWriteStream(config.outputFile, {
    flags: 'a',
    highWaterMark: config.performance.bufferSize,
  });

  let messageBatch = [];
  let messageCount = 0;
  let filteredCount = 0;
  let lastMessageTime = Date.now();

  const flushBatch = async () => {
    if (messageBatch.length > 0) {
      const data = messageBatch.join('\n') + '\n';
      messageBatch = [];
      if (!outputStream.write(data)) {
        console.log('Write buffer full, waiting for drain...');
        await new Promise((resolve) => outputStream.once('drain', resolve));
        console.log('Write buffer drained, continuing...');
      }
    }
  };

  const checkInactivity = () => {
    const now = Date.now();
    if (now - lastMessageTime > config.performance.noActivityTimeout) {
      console.log('No new records found for', config.performance.noActivityTimeout/1000, 'seconds');
      cleanup().then(() => process.exit(0));
    }
  };

  const inactivityInterval = setInterval(checkInactivity, 5000);

  const cleanup = async () => {
    clearInterval(inactivityInterval);
    console.log('Starting cleanup process...');
    await flushBatch();
    console.log('Final batch flushed');
    outputStream.end();
    console.log('Output stream closed');
    await consumer.disconnect();
    console.log('Kafka consumer disconnected');

    const totalTime = process.uptime();
    console.log(`Processing complete:
      Total messages processed: ${messageCount}
      Messages included in output: ${filteredCount}
      Messages excluded: ${messageCount - filteredCount}
      Time taken: ${totalTime.toFixed(2)}s
      Average speed: ${Math.round(messageCount / totalTime)} msg/sec`);
  };

  const processMessage = async ({ message, partition }) => {
    try {
      lastMessageTime = Date.now();
      const messageString = message.value.toString();
      const messageTimestamp = parseInt(message.timestamp);
      messageCount++;

      if (messageCount % 1000 === 0) {
        console.log(`\nRecord Info: Partition: ${partition}, Offset: ${message.offset}, Timestamp: ${new Date(parseInt(message.timestamp)).toISOString()}, Timestamp_: ${messageTimestamp}, Key: ${message.key ? message.key.toString() : 'null'}, Size: ${message.value?.length} bytes`);
      }

      // Check if message is within time range
      if (messageTimestamp < config.filter.timeRange.start || messageTimestamp > config.filter.timeRange.end) {
        if (messageCount % 1000 === 0) {
          console.log(`Message excluded: Outside time range`);
        }
        return;
      }

      if (!config.filter.keyword.split(',').some(keyword => messageString.includes(keyword))) {
        if (messageCount % 1000 === 0) {
          console.log(`Message excluded: Does not contain keyword "${config.filter.keyword}"`);
        }
        return;
      }

      messageBatch.push(messageString);
      filteredCount++;
      if (messageCount % 1000 === 0) {
        console.log(`Message included: Contains keyword "${config.filter.keyword}" and within time range`);
        console.log(`Content preview: ${messageString.substring(0, 100)}...`);
      }

      if (messageBatch.length >= config.performance.batchSize) {
        console.log(`Batch size ${config.performance.batchSize} reached, flushing...`);
        await flushBatch();
      }

      if (messageCount % config.performance.progressInterval === 0) {
        const elapsed = process.uptime();
        console.log(`Progress update:
          Messages processed: ${messageCount}
          Messages included: ${filteredCount}
          Messages excluded: ${messageCount - filteredCount}
          Processing rate: ${Math.round(messageCount / elapsed)} msg/sec`);
      }

    } catch (error) {
      console.error(`Error processing message:`, error);
      console.error('Message details:', {
        offset: message.offset,
        partition,
        timestamp: new Date(parseInt(message.timestamp)).toISOString(),
        size: message.value?.length
      });
    }
  };

  try {
    console.log('Starting message consumption...');

    await consumer.run({
      eachMessage: processMessage,
      eachBatchAutoResolve: true,
    });

    // Seek to start timestamp after consumer.run is called
    const admin = kafka.admin();
    await admin.connect();

    const partitions = await admin.fetchTopicOffsetsByTimestamp(config.topic, config.filter.timeRange.start);

    for (const partition of partitions) {
      if (partition.offset !== undefined) {
        await consumer.seek({
          topic: config.topic,
          partition: partition.partition,
          offset: partition.offset
        });
      }
    }

    await admin.disconnect();

  } catch (error) {
    console.error('Error while consuming messages:', error);
    console.error('Error stack:', error.stack);
    await cleanup();
  }

  process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT signal');
    console.log('Gracefully shutting down...');
    await cleanup();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\nReceived SIGTERM signal');
    console.log('Gracefully shutting down...');
    await cleanup();
    process.exit(0);
  });
}

consumeMessages()
  .then(() => console.log('Kafka consumer started...'))
  .catch((err) => {
    console.error('Critical error starting Kafka consumer:', err);
    console.error('Error stack:', err.stack);
    process.exit(1);
  });
