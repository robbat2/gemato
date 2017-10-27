# gemato: File verification routines
# vim:fileencoding=utf-8
# (c) 2017 Michał Górny
# Licensed under the terms of 2-clause BSD license

import errno
import fcntl
import os
import stat

import gemato.exceptions
import gemato.hash
import gemato.manifest


def get_file_metadata(path, hashes):
    """
    Get a generator for the metadata of the file at system path @path.

    The generator yields, in order:
    1. A boolean indicating whether the file exists.
    2. st_dev, if the file exists.
    3. Tuple of (S_IFMT(st_mode), file type as string), if the file
       exists.
    4. st_size, if the file exists and is a regular file. Note that
       it may be 0 on some filesystems, so treat the value with caution.
    5. A dict of @hashes and their values, if the file exists and is
       a regular file. Special __size__ member is added unconditionally.

    Note that the generator acquires resources, and does not release
    them until terminated. Always make sure to pull it until
    StopIteration, or close it explicitly.
    """

    try:
        # we want O_NONBLOCK to avoid blocking when opening pipes
        fd = os.open(path, os.O_RDONLY|os.O_NONBLOCK)
    except OSError as err:
        if err.errno == errno.ENOENT:
            exists = False
            opened = False
        elif err.errno == errno.ENXIO:
            # unconnected device or socket
            exists = True
            opened = False
        else:
            raise
    else:
        exists = True
        opened = True

    try:
        # 1. does it exist?
        yield exists

        # we can't provide any more data for a file that does not exist
        if not exists:
            return

        if opened:
            st = os.fstat(fd)
        else:
            st = os.stat(path)

        # 2. st_dev
        yield st.st_dev

        # 3. file type tuple
        if stat.S_ISREG(st.st_mode):
            ftype = 'regular file'
        elif stat.S_ISDIR(st.st_mode):
            ftype = 'directory'
        elif stat.S_ISCHR(st.st_mode):
            ftype = 'character device'
        elif stat.S_ISBLK(st.st_mode):
            ftype = 'block device'
        elif stat.S_ISFIFO(st.st_mode):
            ftype = 'named pipe'
        elif stat.S_ISSOCK(st.st_mode):
            ftype = 'UNIX socket'
        else:
            ftype = 'unknown'
        yield (stat.S_IFMT(st.st_mode), ftype)

        if not stat.S_ISREG(st.st_mode):
            if opened:
                os.close(fd)
            return

        # 4. st_size
        yield st.st_size

        f = os.fdopen(fd, 'rb')
    except:
        if opened:
            os.close(fd)
        raise

    with f:
        # open() might have left the file as O_NONBLOCK
        # make sure to fix that
        fcntl.fcntl(fd, fcntl.F_SETFL, 0)

        # 5. checksums
        e_hashes = sorted(hashes)
        hashes = list(gemato.manifest.manifest_hashes_to_hashlib(e_hashes))
        e_hashes.append('__size__')
        hashes.append('__size__')
        checksums = gemato.hash.hash_file(f, hashes)

        ret = {}
        for ek, k in zip(e_hashes, hashes):
            ret[ek] = checksums[k]
        yield ret


def verify_path(path, e, expected_dev=None):
    """
    Verify the file at system path @path against the data in entry @e.
    The path/filename is not matched against the entry -- the correct
    entry must be passed by the caller.

    If the path passes verification, returns (True, []). Otherwise,
    returns (False, diff) where diff is a list of differences between
    the file at path and the Manifest entry. Each list element is
    a tuple of (name, expected, got).

    If @expected_dev is not None, verifies that the file resides
    on specified device. If the device does not match, raises
    ManifestCrossDevice exception. It can be used to verify that
    the files do not cross filesystem boundaries.

    Each name can be:
    - __exists__ (boolean) to indicate whether the file existed,
    - __type__ (string) as a human-readable description of file type,
    - __size__ (int) as file size,
    - any checksum name according to the entry.
    """

    if e is not None:
        assert isinstance(e, gemato.manifest.ManifestPathEntry)

        # IGNORE entries cause verification to always succeed
        if isinstance(e, gemato.manifest.ManifestEntryIGNORE):
            return (True, [])

    try:
        # we want O_NONBLOCK to avoid blocking when opening pipes
        fd = os.open(path, os.O_RDONLY|os.O_NONBLOCK)
    except OSError as err:
        if err.errno == errno.ENOENT:
            exists = False
            opened = False
        elif err.errno == errno.ENXIO:
            # unconnected device or socket
            exists = True
            opened = False
        else:
            raise
    else:
        exists = True
        opened = True

    # 1. verify whether the file existed in the first place
    expect_exist = (e is not None
            and not isinstance(e, gemato.manifest.ManifestEntryOPTIONAL))
    if exists != expect_exist:
        if opened:
            os.close(fd)
        return (False, [('__exists__', expect_exist, exists)])
    elif not exists:
        return (True, [])

    # 2. verify whether the file is a regular file
    if opened:
        st = os.fstat(fd)
    else:
        st = os.stat(path)
    if expected_dev is not None and st.st_dev != expected_dev:
        if opened:
            os.close(fd)
        raise gemato.exceptions.ManifestCrossDevice(path)
    if not opened or not stat.S_ISREG(st.st_mode):
        if opened:
            os.close(fd)
        if stat.S_ISDIR(st.st_mode):
            ftype = 'directory'
        elif stat.S_ISCHR(st.st_mode):
            ftype = 'character device'
        elif stat.S_ISBLK(st.st_mode):
            ftype = 'block device'
        elif stat.S_ISREG(st.st_mode):  # can only happen w/ ENXIO
            ftype = 'unconnected regular file (?!)'
        elif stat.S_ISFIFO(st.st_mode):
            ftype = 'named pipe'
        elif stat.S_ISSOCK(st.st_mode):
            ftype = 'UNIX socket'
        else:
            ftype = 'unknown'
        return (False, [('__type__', 'regular file', ftype)])

    # grab the fd
    try:
        f = os.fdopen(fd, 'rb')
    except Exception:
        os.close(fd)
        raise

    with f:
        # open() might have left the file as O_NONBLOCK
        # make sure to fix that
        fcntl.fcntl(fd, fcntl.F_SETFL, 0)

        # ignore st_size == 0 in case of weird filesystem
        if st.st_size != 0 and st.st_size != e.size:
            return (False, [('__size__', e.size, st.st_size)])

        e_hashes = sorted(e.checksums)
        hashes = list(gemato.manifest.manifest_hashes_to_hashlib(e_hashes))
        hashes.append('__size__')
        checksums = gemato.hash.hash_file(f, hashes)

        diff = []
        size = checksums['__size__']
        if size != e.size:
            diff.append(('__size__', e.size, size))
        for ek, k in zip(e_hashes, hashes):
            exp = e.checksums[ek]
            got = checksums[k]
            if got != exp:
                diff.append((ek, exp, got))

        if diff:
            return (False, diff)

    return (True, [])


def verify_entry_compatibility(e1, e2):
    """
    Verify that the two entries @e1 and @e2 are compatible.

    If the entries are compatible, returns (True, diff). Otherwise,
    returns (False, diff). Here diff is a list of differences between
    @e1 and @e2. Each list element is a tuple of (name, e1, e2).

    In case of successful comparison, the diff may contain additional
    hashes that are present only in one of the entries.
    """

    assert isinstance(e1, gemato.manifest.ManifestPathEntry)
    assert isinstance(e2, gemato.manifest.ManifestPathEntry)

    # 1. compare types
    t1 = e1.tag
    t2 = e2.tag
    if t1 != t2:
        # all those tags have compatible semantics
        COMPATIBLE_TAGS = ('MANIFEST', 'DATA', 'EBUILD', 'AUX')
        if t1 not in COMPATIBLE_TAGS or t2 not in COMPATIBLE_TAGS:
            return (False, [('__type__', t1, t2)])

    # 2. compare sizes
    if e1.size != e2.size:
        return (False, [('__size__', e1.size, e2.size)])

    # 3. compare checksums
    hashes = frozenset(e1.checksums) | frozenset(e2.checksums)
    ret = True
    diff = []
    for h in sorted(hashes):
        h1 = e1.checksums.get(h)
        h2 = e2.checksums.get(h)
        if h1 != h2:
            diff.append((h, h1, h2))
            if h1 is not None and h2 is not None:
                ret = False

    return (ret, diff)
