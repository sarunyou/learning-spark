import * as util from 'util';
import * as stream from 'stream';
import * as fs from 'fs';
import { once } from 'events';

const finished = util.promisify(stream.finished); // (A)

async function writeIterableToFile(filePath) {
  const writable = fs.createWriteStream(filePath, { encoding: 'utf8' });
  const mod = 10_000_000;
  for (let index = 0; index < 20_000_000; index++) {
    if (!writable.write(`${index % mod}\n`)) {
      // Handle backpressure
      await once(writable, 'drain');
    }
  }
  writable.end(); // (C)
  // Wait until done. Throws if there are errors.
  await finished(writable);
}

await writeIterableToFile('mock.csv');
