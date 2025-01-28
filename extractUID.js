const fs = require('fs');

// Path to your .txt file
const filePath = 'kafka_messages.json';

// Regular expression to match the UID field in the features object
const uidRegex = /"features"\s*:\s*\[.*?"UID"\s*:\s*"([^"]+)".*?\]/g;

// Read the file
fs.readFile(filePath, 'utf8', (err, data) => {
  if (err) {
    console.error('Error reading the file:', err);
    return;
  }

  // Array to store UIDs
  const uids = [];

  // Split file content into lines
  const lines = data.split('\n');

  // Process each line
  lines.forEach((line) => {
    let match;
    while ((match = uidRegex.exec(line)) !== null) {
      uids.push(match[1]); // Extract the UID value (first capturing group)
    }
  });

  // Write UIDs in batches of 500 per line
  const batchSize = 500;
  const outputLines = [];

  for (let i = 0; i < uids.length; i += batchSize) {
    // Get a slice of 500 UIDs
    const batch = uids.slice(i, i + batchSize);
    // Join the batch into a single line
    outputLines.push(batch.join(','));
  }

  // Write the output to a file
  const outputFilePath = 'uids_output.txt';
  fs.writeFileSync(outputFilePath, outputLines.join('\n'), 'utf-8');

  console.log(`UIDs have been written to ${outputFilePath} in batches of ${batchSize}.`);
});
