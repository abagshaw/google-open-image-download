from __future__ import unicode_literals

import argparse
import csv
import errno
import logging
import multiprocessing
import os
import shutil
import time
import traceback

from PIL import Image

import six

#Authenticate with GCP and setup service
from google.cloud import storage
storage_client = storage.Client()
# Create a bucket object for our bucket
bucket = storage_client.bucket("open-images-dataset")

def config_logger():
    logger = logging.getLogger('download')
    logger.setLevel(logging.WARNING)

    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)

    formatter = logging.Formatter('%(process)d @ %(asctime)s (%(relativeCreated)d) '
                                  '%(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


def parse_args():
    parser = argparse.ArgumentParser(description='Download Google Open Images dataset from GCP bucket.')

    parser.add_argument('--timeout', type=float, default=2.0,
                        help='image download timeout')
    parser.add_argument('--queue-size', type=int, default=5000,
                        help='maximum image url queue size')
    parser.add_argument('--consumers', type=int, default=32,
                        help='number of download workers')
    parser.add_argument('--min-dim', type=int, default=320,
                        help='smallest dimension for the aspect ratio preserving scale'
                             '(-1 for no scale)')
    parser.add_argument('--force', default=False, action='store_true',
                        help='force download and overwrite local files')
    parser.add_argument('--download-folder', default="/", help='folder in bucket where images are to be found.')

    parser.add_argument('input', help='open image input csv')
    parser.add_argument('output', help='save directory')

    return parser.parse_args()


def unicode_dict_reader(f, **kwargs):
    csv_reader = csv.DictReader(f, **kwargs)
    for row in csv_reader:
        yield {key: value for key, value in six.iteritems(row)}


def safe_mkdir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            log.exception()


def make_out_path(code, out_dir):
    safe_mkdir(out_dir)

    return os.path.join(out_dir, code + '.jpg')


def scale(content, min_dim):
    """ Aspect-ratio preserving scale such that the smallest dim is equal to `min_dim` """

    image = Image.open(content)

    # no scaling, keep images full size
    if min_dim == -1:
        return image

    # aspect-ratio preserving scale so that the smallest dimension is `min_dim`
    width, height = image.size
    scale_dimension = width if width < height else height
    scale_ratio = float(min_dim) / scale_dimension

    if scale_ratio == 1:
        return image

    return image.resize(
        (int(width * scale_ratio), int(height * scale_ratio)),
        Image.ANTIALIAS,
    )


def read_image(path, min_dim):
    """ Download response in chunks and convert to a scaled Image object """

    # Create a blob object from the filepath
    blob = bucket.blob(path)
    # Download the file to a destination
    content = six.BytesIO(blob.download_as_string())
    content.seek(0)

    return scale(content, min_dim)


def consumer(args, queue):
    """ Whilst the queue has images, download and save them """

    while queue.empty():
        time.sleep(0.1)  # give the queue a chance to populate

    while not queue.empty():
        code = queue.get(block=True, timeout=None)

        out_path = make_out_path(code, args.output)

        if not args.force and os.path.exists(out_path):
            log.debug('skipping {}, already exists'.format(out_path))
            continue

        try:
            image = read_image("{}/{}.jpg".format(args.download_folder, code), args.min_dim)
            image.save(out_path)
        except Exception:
            log.warning('error {}'.format(traceback.format_exc()))
        else:
            log.debug('saving {} to {}'.format(code, out_path))


def producer(args, queue):
    """ Populate the queue with image_id, url pairs. """

    with open(args.input) as f:
        prev = None
        for row in unicode_dict_reader(f):
            if row['ImageID'] != prev:
                queue.put(row['ImageID'], block=True, timeout=None)
                prev = row['ImageID']
                log.debug('queue_size = {}'.format(queue.qsize()))

    queue.close()


log = config_logger()


if __name__ == '__main__':
    args = parse_args()
    log.debug(args)

    queue = multiprocessing.Queue(args.queue_size)

    processes = [
        multiprocessing.Process(target=producer, args=(args, queue))
    ]

    for i in range(args.consumers):
        processes.append(multiprocessing.Process(target=consumer, args=(args, queue)))

    for p in processes:
        p.start()

    for p in processes:
        p.join()
