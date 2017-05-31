from __future__ import absolute_import, unicode_literals

import logging
import numpy
import pickle
import sys
import csv
from diplom.models.CRNN.model import MRCC
from diplom.models.CRNN.utils import category_to_ar

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
csv.field_size_limit(sys.maxsize)


def process_row(row):
    inp = pickle.loads(row[0])
    genre = row[1]

    return numpy.expand_dims(inp.T, axis=2), numpy.array(category_to_ar(genre))


def get_data_generator(filename, row_count=100):
    """
    :type filename: unicode | str
    :type row_count: int
    :rtype: tuple
    """
    with open(filename, 'r') as f:
        while True:
            f.seek(0)
            reader = csv.reader(f)
            a = []
            b = []
            for row in reader:
                x, y = process_row(row)
                a.append(x)
                b.append(y)
                if len(a) >= row_count:
                    xx = numpy.array(a)
                    yy = numpy.array(b)
                    a = []
                    b = []
                    yield xx, yy
                    xx = None
                    yy = None

            if a:
                xx = numpy.array(a)
                yy = numpy.array(b)
                a = []
                b = []
                yield xx, yy
                xx = None
                yy = None


def get_train_gen():
    generator = get_data_generator('calculated_mfccs/fit.csv')
    samples = 64
    return generator, samples


def get_validate_gen():
    generator = get_data_generator('calculated_mfccs/validate.csv')
    samples = 15
    return generator, samples


def get_evaluate_gen():
    generator = get_data_generator('calculated_mfccs/test.csv')
    samples = 21
    return generator, samples


def run():
    logger.info('Creating model')
    model = MRCC(include_top=True)

    logger.info('Compiling model')
    model.compile(
        optimizer='adam',
        loss='binary_crossentropy',
        metrics=['accuracy'],
    )

    logger.info('Fit model')
    train_generator, train_samples = get_train_gen()
    validate_generator, validate_samples = get_validate_gen()
    model.fit_generator(
        train_generator,
        epochs=50,
        samples_per_epoch=train_samples,
        verbose=2,
        validation_data=validate_generator,
        validation_steps=validate_samples,
    )

    logger.info('Saving model')
    model.save('saved_model', overwrite=True)
    model.save_weights('saved_weights', overwrite=True)

    evaluate_generator, evaludate_samples = get_evaluate_gen()
    result = model.evaluate_generator(
        evaluate_generator,
        steps=evaludate_samples
    )
    print result


if __name__ == '__main__':
    run()
