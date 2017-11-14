#!/usr/bin/env python
# (c) 2017 Michał Górny
# (c) 2017 Robin H. Johnson <robbat2@gentoo.org>
# Licensed under the terms of 2-clause BSD license
# vim: ts=4 sts=4 sw=4 et ft=python:

import glob
import io
import os
import os.path
import sys
import multiprocessing
import argparse
import logging
import re
import itertools
logger = multiprocessing.log_to_stderr()
logger.setLevel(logging.INFO)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import gemato.hash

class PathType(object):
    def __init__(self, exists=True, type='file', dash_ok=True):
        '''exists:
                True: a path that does exist
                False: a path that does not exist, in a valid parent directory
                None: don't care
           type: file, dir, symlink, None, or a function returning True for valid paths
                None: don't care
           dash_ok: whether to allow "-" as stdin/stdout'''

        assert exists in (True, False, None)
        assert type in ('file','dir','symlink',None) or hasattr(type,'__call__')

        self._exists = exists
        self._type = type
        self._dash_ok = dash_ok

    def __call__(self, string):
        if string=='-':
            # the special argument "-" means sys.std{in,out}
            if self._type == 'dir':
                raise err('standard input/output (-) not allowed as directory path')
            elif self._type == 'symlink':
                raise err('standard input/output (-) not allowed as symlink path')
            elif not self._dash_ok:
                raise err('standard input/output (-) not allowed')
        else:
            e = os.path.exists(string)
            if self._exists==True:
                if not e:
                    raise err("path does not exist: '%s'" % string)

                if self._type is None:
                    pass
                elif self._type=='file':
                    if not os.path.isfile(string):
                        raise err("path is not a file: '%s'" % string)
                elif self._type=='symlink':
                    if not os.path.symlink(string):
                        raise err("path is not a symlink: '%s'" % string)
                elif self._type=='dir':
                    if not os.path.isdir(string):
                        raise err("path is not a directory: '%s'" % string)
                elif not self._type(string):
                    raise err("path not valid: '%s'" % string)
            else:
                if self._exists==False and e:
                    raise err("path exists: '%s'" % string)

                p = os.path.dirname(os.path.normpath(string)) or '.'
                if not os.path.isdir(p):
                    raise err("parent path is not a directory: '%s'" % p)
                elif not os.path.exists(p):
                    raise err("parent directory does not exist: '%s'" % p)

        return string


def write_manifest_entry(manifest_file, t, path, relpath, hashes):
    checksums = gemato.hash.hash_path(path,
            [x.lower() for x in hashes] + ['__size__'])
    hashvals = []
    for h in hashes:
        hashvals += [h, checksums[h.lower()]]
    manifest_file.write('{} {} {} {}\n'.format(t, relpath,
        checksums['__size__'], ' '.join(hashvals)))


def write_manifest_entries_for_dir_filename(manifest_filename, topdir, hashes):
    logger.debug("START dir {}".format(topdir))
    with io.open(manifest_filename, 'w') as f:
        write_manifest_entries_for_dir_fileobj(f, topdir, hashes)
    logger.debug("DONE dir {}".format(topdir))


def write_manifest_entries_for_dir_fileobj(manifest_file, topdir, hashes):
    for dirpath, dirs, files in os.walk(topdir):
        if dirpath != topdir:
            for f in files:
                if f.startswith('Manifest'):
                    fp = os.path.join(dirpath, f)
                    write_manifest_entry(manifest_file, 'MANIFEST',
                            fp, os.path.relpath(fp, topdir), hashes)
                    # do not descend
                    dirs.clear()
                    skip = True
                    break
            else:
                skip = False
            if skip:
                continue

        for f in files:
            if f.startswith('Manifest'):
                continue
            fp = os.path.join(dirpath, f)
            write_manifest_entry(manifest_file, 'DATA',
                    fp, os.path.relpath(fp, topdir), hashes)

def async_write_manifest_entries_for_dir_filename(manifest_filename, bmdir, hashes):
    write_manifest_entries_for_dir_filename(manifest_filename, bmdir, hashes)
    return bmdir


def gen_metamanifests(top_dir, hashes, **kwargs):
    with io.open(os.path.join(top_dir, 'profiles/categories'), 'r') as f:
        categories = [x.strip() for x in f]

    # Ideally, we would be able to automate all of this dependency stuff,
    # by using a topological sort, and processing (and gradually removing) all
    # nodes with no children
    # But lacking that, we know the structure of the tree very well
    # So we can manually construct an optimal set
    passes = [[]]
    # we assume every package has thick Manifests already, so we just
    # need to Manifest the categories
    passes[0].extend(categories)
    # We also want per-category metadata Manifests
    passes[0].extend([os.path.join('metadata/md5-cache', c) for c in categories])
    # And we can Manifest anything else with no dependencies
    passes[0].extend([
        'eclass',
        'licenses',
        'metadata/dtd',
        'metadata/glsa',
        'metadata/news',
        'metadata/repoman',
        'metadata/xml-schema',
        'profiles',
        ])
    # Now step up towards root directory
    # Optimally, metadata/md5-cache, could run as SOON as the metadata categories have completed.
    passes.append(['metadata/md5-cache'])
    passes.append(['metadata'])
    passes.append([''])

    threads = kwargs.pop('threads', None)
    pool = multiprocessing.Pool(processes=threads)
    results = {}
    func = async_write_manifest_entries_for_dir_filename

    def schedule_manifest(*args):
        if threads == 1:
            func(*args)
            return None
        else:
            return pool.apply_async(func, args)

    for alldirs in passes:
        results = {}
        for bm in alldirs:
            bmdir = os.path.join(top_dir, bm)
            manifest_filename = os.path.join(bmdir, 'Manifest')
            if not list(glob.glob(manifest_filename + '*')):
                r = schedule_manifest(manifest_filename, bmdir, hashes)
                if r:
                    results[bmdir] = r
        failed = []
        for bmdir, r in results.items():
            if not r.ready():
                r.wait()
            if not r.successful():
                failed.append("r.ready={} r.successful={} Failed for {}".format(r.ready(), r.successful(), str(func), bmdir))
        if failed:
            for s in failed:
                print(s)
            sys.exit(1)

    # Wait for all jobs
    # The pool SHOULD be empty at this point
    pool.close()
    pool.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--threads', type=int, default=None, help='numbers of threads')
    dirtype = PathType(exists=True, type='dir')
    parser.add_argument('top_dir', metavar='RSYNC-PATH', type=dirtype, help='rsync path')
    parser.add_argument('--hashes', metavar='HASHES', type=str, help='hashes', nargs='+', default=['SHA256', 'SHA512', 'WHIRLPOOL'])

    args = parser.parse_args()
    # Special case, no-hashes is valid
    args.hashes = list(itertools.chain.from_iterable([re.split('[, ]+', x) for x in args.hashes]))
    if args.hashes == ['']:
        args.hashes = []

    print(args)

    kwargs = vars(args)
    gen_metamanifests(**kwargs)
