# gemato: compressed file support
# vim:fileencoding=utf-8
# (c) 2017 Michał Górny
# Licensed under the terms of 2-clause BSD license

import gzip
import io
import os.path
import sys

if sys.version_info >= (3, 3):
    import bz2
else:
    # older bz2 module versions do not handle multiple streams correctly
    # so use the backport instead
    try:
        import bz2file as bz2
    except ImportError:
        bz2 = None

try:
    import lzma
except ImportError:
    try:
        import backports.lzma as lzma
    except ImportError:
        lzma = None

import gemato.exceptions


def open_compressed_file(suffix, f, mode='r'):
    """
    Get a file-like object for an open compressed file @fileobj
    of format @suffix. The file should be open in binary mode
    and positioned at the beginning. @suffix should specify a standard
    suffix for the compression format without the leading dot,
    e.g. "gz", "bz2".
    """

    if suffix == "gz":
        return gzip.GzipFile(fileobj=f, mode=mode)
    elif suffix == "bz2" and bz2 is not None:
        return bz2.BZ2File(f, mode=mode)
    elif suffix == "lzma" and lzma is not None:
        return lzma.LZMAFile(f, format=lzma.FORMAT_ALONE, mode=mode)
    elif suffix == "xz" and lzma is not None:
        return lzma.LZMAFile(f, format=lzma.FORMAT_XZ, mode=mode)

    raise gemato.exceptions.UnsupportedCompression(suffix)


class FileStack(object):
    """
    A context manager for stacked files. Maintains handles for all files
    on stack, returns the topmost (last) layer on enter and closes them
    all on exit.
    """

    def __init__(self, files=[]):
        self.files = files

    def __enter__(self):
        return self.files[-1]

    def __exit__(self, exc_type, exc_value, exc_cb):
        self.close()

    def close(self):
        for f in reversed(self.files):
            f.close()


def open_potentially_compressed_path(path, mode, **kwargs):
    """
    Open the potentially compressed file at specified path @path
    with mode @mode. If the path ends with one of the known compression
    suffixes, the file will be decompressed transparently. Otherwise,
    it will be open directly.

    @kwargs can be used to pass additional options for text files.
    Only arguments supported by io.TextIOWrapper should be used there.

    Returns an object that must be used via the context manager API.
    """

    base, ext = os.path.splitext(path)
    if ext not in ('.gz', '.bz2', '.lzma', '.xz'):
        return io.open(path, mode, **kwargs)

    bmode = mode
    if 'b' not in bmode:
        bmode += 'b'

    f = io.open(path, bmode)
    fs = FileStack([f])
    try:
        cf = open_compressed_file(ext[1:], f, bmode if kwargs else mode)
        fs.files.append(cf)

        # special args are not supported by compressor backends
        # so add a TextIOWrapper on top
        if kwargs:
            iow = io.TextIOWrapper(cf, **kwargs)
            fs.files.append(iow)
    except:
        fs.close()
        raise

    return fs