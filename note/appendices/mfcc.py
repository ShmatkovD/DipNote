from __future__ import unicode_literals, absolute_import

import numpy
import logging
from essentia import standard as esd

logger = logging.getLogger(__name__)


def get_mfcc_bands(item_path):
    logger.info('Getting mfcc bands from %s', item_path)
    # loading record
    mono_loader = esd.MonoLoader(filename=item_path)
    load_result = mono_loader.compute()

    # cutting on frames
    frame_size = len(load_result) / 1366
    frames = [
        frame for frame in
        esd.FrameGenerator(
            load_result,
            frameSize=frame_size * 2,
            hopSize=frame_size
        )
    ][:1366]

    # windowing frames
    window = esd.Windowing(type='hann')
    windowed_frames = [window(frame) for frame in frames]

    # spectrum calculation
    spectrum = esd.Spectrum()
    specs = [spectrum(frame) for frame in windowed_frames]

    # calculation mfcc
    mfcc = esd.MFCC(inputSize=20000, numberCoefficients=96, numberBands=96)
    mfcc_coeffs = []
    mfcc_bands = []
    for spectrum in specs:
        mfcc_band, mfcc_coeff = mfcc(spectrum)
        mfcc_coeffs.append(mfcc_coeff)
        mfcc_bands.append(mfcc_band)

    bands_array = numpy.array(mfcc_bands)

    return bands_array
