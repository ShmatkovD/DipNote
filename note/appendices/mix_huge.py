import csv
import logging
import os
import random
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

csv.field_size_limit(sys.maxsize)


def mix_batches(batch1, batch2):
    logger.info('Reading datasets')
    with open(batch1, 'r') as in1:
        with open(batch2, 'r') as in2:
            reader1 = csv.reader(in1)
            reader2 = csv.reader(in2)
            gen1 = reader1.__iter__()
            gen2 = reader2.__iter__()

            new_batch = []

            try:
                while True:
                    value = next(gen1)
                    new_batch.append(value)

                    value = next(gen2)
                    new_batch.append(value)
            except StopIteration:
                pass

            try:
                while True:
                    value = next(gen1)
                    new_batch.append(value)
            except StopIteration:
                pass


            try:
                while True:
                    value = next(gen2)
                    new_batch.append(value)
            except StopIteration:
                pass

    logger.info('Writing datasets')

    with open('{}.tmp'.format(batch1), 'w') as out1:
        with open('{}.tmp'.format(batch2), 'w') as out2:
            writer1 = csv.writer(out1)
            writer2 = csv.writer(out2)

            for i, item in enumerate(new_batch):
                if i % 2 == 0:
                    writer1.writerow(item)
                else:
                    writer2.writerow(item)

    logger.info('Moving files')
    os.remove(batch1)
    os.remove(batch2)

    os.rename('{}.tmp'.format(batch1), batch1)
    os.rename('{}.tmp'.format(batch2), batch2)


def mix_randomly(batch1, batch2):

    full_batch = []

    logger.info('Reading datasets')
    with open(batch1, 'r') as inf:
        reader = csv.reader(inf)
        for row in reader:
            full_batch.append(row)

    with open(batch2, 'r') as inf:
        reader = csv.reader(inf)
        for row in reader:
            full_batch.append(row)

    logger.info('Shuffle dataset')
    random.shuffle(full_batch)

    logger.info('Writing dataset')
    with open('{}.tmp'.format(batch1), 'w') as in1:
        with open('{}.tmp'.format(batch2), 'w') as in2:
            writer1 = csv.writer(in1)
            writer2 = csv.writer(in2)

            for i, item in enumerate(full_batch):
                if i % 2 == 0:
                    writer1.writerow(item)
                else:
                    writer2.writerow(item)

    logger.info('Moving files')
    os.remove(batch1)
    os.remove(batch2)

    os.rename('{}.tmp'.format(batch1), batch1)
    os.rename('{}.tmp'.format(batch2), batch2)


def run(src, dest):

    dir_content = sorted(os.listdir(src))

    logger.info('Making mixing things')

    pairs = []
    for i in dir_content:
        for j in dir_content:
            if i == j: continue
            pairs.append((i, j))

    # logger.info('Mixing')
    # for i in xrange(3):
    #     logger.info('# %s mix', i)
    #     for batch1, batch2 in pairs:
    #         logger.info('Mixing %s, %s', batch1, batch2)
    #         batch1_path = os.path.join(src, batch1)
    #         batch2_path = os.path.join(src, batch2)
    #         mix_batches(batch1_path, batch2_path)

    logger.info('Random mix')
    for batch1, batch2 in pairs:
        logger.info('Mixing %s, %s', batch1, batch2)
        batch1_path = os.path.join(src, batch1)
        batch2_path = os.path.join(src, batch2)
        mix_randomly(batch1_path, batch2_path)

    logger.info('Merging dataset')
    with open(dest, 'w') as outf:
        writer = csv.writer(outf)
        for batch in dir_content:
            logger.info('Merge %s', batch)
            batch_path = os.path.join(src, batch)

            with open(batch_path, 'r') as inf:
                reader = csv.reader(inf)

                for row in reader:
                    writer.writerow(row)


if __name__ == '__main__':
    src = sys.argv[1]
    dest = sys.argv[2]

    run(src, dest)
