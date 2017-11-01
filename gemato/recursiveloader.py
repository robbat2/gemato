# gemato: Recursive loader for Manifests
# vim:fileencoding=utf-8
# (c) 2017 Michał Górny
# Licensed under the terms of 2-clause BSD license

import errno
import os.path

import gemato.compression
import gemato.exceptions
import gemato.manifest
import gemato.profile
import gemato.util
import gemato.verify


class ManifestRecursiveLoader(object):
    """
    A class encapsulating a tree covered by multiple Manifests.
    Automatically verifies and loads additional sub-Manifests,
    and provides methods to access the entries in them.
    """

    __slots__ = ['root_directory', 'loaded_manifests', 'verify_openpgp',
            'openpgp_env', 'sign_openpgp', 'openpgp_keyid', 'hashes',
            'openpgp_signed', 'updated_manifests', 'manifest_device',
            'profile']

    def __init__(self, top_manifest_path,
            verify_openpgp=True, openpgp_env=None,
            sign_openpgp=None, openpgp_keyid=None,
            hashes=None, allow_create=False,
            profile=gemato.profile.DefaultProfile()):
        """
        Instantiate the loader for a Manifest tree starting at top-level
        Manifest @top_manifest_path.

        @verify_openpgp and @openpgp_env are passed down
        to ManifestFile. If the top-level Manifest is OpenPGP-signed
        and the verification succeeds, openpgp_signed property
        is set to True.

        @sign_openpgp is passed down to ManifestFile when writing
        the top-level Manifest. If it is True, the top-level Manifest
        will be signed. If it is False, it will not be signed.
        If it is left as None, then it will be signed if it was
        originally signed. @openpgp_keyid can be used to select the key.

        Sub-Manifests are never signed.

        @hashes can be used to specify a default hash set
        for the Manifest. If it is specified, they will be used for all
        subsequent update*() calls that do not specify another set
        of hashes explicitly.

        If @allow_create is True and @top_manifest_path does not exist,
        a new Manifest tree will be initialized. Otherwise, opening
        a non-existing file will cause an exception.

        @profile can be used to provide the profile for the repository.
        """

        self.root_directory = os.path.dirname(top_manifest_path)
        self.verify_openpgp = verify_openpgp
        self.openpgp_env = openpgp_env
        self.sign_openpgp = sign_openpgp
        self.openpgp_keyid = openpgp_keyid
        self.hashes = hashes
        self.profile = profile

        self.loaded_manifests = {}
        self.updated_manifests = set()

        # TODO: allow catching OpenPGP exceptions somehow?
        m = self.load_manifest(os.path.basename(top_manifest_path),
                allow_create=allow_create)
        self.openpgp_signed = m.openpgp_signed

    def load_manifest(self, relpath, verify_entry=None,
            allow_create=False):
        """
        Load a single Manifest file whose relative path within Manifest
        tree is @relpath. If @verify_entry is not null, the Manifest
        file is verified against the entry. If the file is compressed,
        it is decompressed transparently.

        If @allow_create is True and the Manifest does not exist,
        a new Manifest will be added. Otherwise, opening a non-existing
        file will cause an exception.
        """
        m = gemato.manifest.ManifestFile()
        path = os.path.join(self.root_directory, relpath)
        if verify_entry is not None:
            ret, diff = gemato.verify.verify_path(path, verify_entry)
            if not ret:
                raise gemato.exceptions.ManifestMismatch(
                        relpath, verify_entry, diff)
        try:
            with gemato.compression.open_potentially_compressed_path(
                    path, 'r', encoding='utf8') as f:
                m.load(f, self.verify_openpgp, self.openpgp_env)
                st = os.fstat(f.fileno())
        except IOError as err:
            if err.errno == errno.ENOENT and allow_create:
                st = os.stat(os.path.dirname(path))
                # trigger saving
                self.updated_manifests.add(relpath)
            else:
                raise err

        self.manifest_device = st.st_dev
        self.loaded_manifests[relpath] = m
        return m

    def save_manifest(self, relpath, sort=False):
        """
        Save a single Manifest file whose relative path within Manifest
        tree is @relpath. The Manifest must already be loaded.
        If the name indicates compression, it will be compressed
        transparently. If it was OpenPGP-signed, a new signature
        will be created.

        If @sort is True, the Manifest entries will be sorted prior
        to saving.

        Returns the uncompressed size of the Manifest (number
        of characters written).
        """
        m = self.loaded_manifests[relpath]
        path = os.path.join(self.root_directory, relpath)

        # is it top-level Manifest?
        if relpath in (gemato.compression
                .get_potential_compressed_names('Manifest')):
            sign = self.sign_openpgp
        else:
            sign = False

        with gemato.compression.open_potentially_compressed_path(
                path, 'w', encoding='utf8') as f:
            m.dump(f, sign_openpgp=sign, sort=sort,
                    openpgp_env=self.openpgp_env,
                    openpgp_keyid=self.openpgp_keyid)
            return f.buffer.tell()

    def _iter_unordered_manifests_for_path(self, path, recursive=False):
        """
        Iterate over loaded Manifests that can apply to path.
        If @recursive is True, returns also Manifests for subdirectories
        of @path. Yields a tuple of (manifest_path, dir_path, manifest).

        The entries will be returned in any order.
        """
        for k, v in self.loaded_manifests.items():
            d = os.path.dirname(k)
            if gemato.util.path_starts_with(path, d):
                yield (k, d, v)
            elif recursive and gemato.util.path_starts_with(d, path):
                yield (k, d, v)

    def _iter_manifests_for_path(self, path, recursive=False):
        """
        Iterate over loaded Manifests that can apply to path.
        If @recursive is True, returns also Manifests for subdirectories
        of @path. Yields a tuple of (manifest_path, dir_path, manifest).

        The function guarantees that the Manifests for subdirectories
        (more specific) will always be returned before the Manifests
        for parent directories. The order is otherwise undefined.
        """
        return sorted(
                self._iter_unordered_manifests_for_path(
                    path, recursive=recursive),
                key=lambda kdv: len(kdv[1]),
                reverse=True)

    def load_manifests_for_path(self, path, recursive=False):
        """
        Load all Manifests that may apply to the specified path,
        recursively. If @recursive is True, also loads Manifests
        for all subdirectories of @path.
        """
        # TODO: figure out how to avoid confusing uses of 'recursive'
        while True:
            to_load = []
            for curmpath, relpath, m in self._iter_manifests_for_path(
                                            path, recursive):
                for e in m.entries:
                    if e.tag != 'MANIFEST':
                        continue
                    mpath = os.path.join(relpath, e.path)
                    if curmpath == mpath or mpath in self.loaded_manifests:
                        continue
                    mdir = os.path.dirname(mpath)
                    if gemato.util.path_starts_with(path, mdir):
                        to_load.append((mpath, e))
                    elif recursive and gemato.util.path_starts_with(mdir, path):
                        to_load.append((mpath, e))
            if not to_load:
                break
            for mpath, e in to_load:
                self.load_manifest(mpath, e)

    def find_timestamp(self):
        """
        Find a timestamp entry and return it. Returns None if there
        is no timestamp.
        """

        self.load_manifests_for_path('')
        for mpath, p, m in self._iter_manifests_for_path(''):
            for e in m.entries:
                if e.tag == 'TIMESTAMP':
                    return e
        return None

    def find_path_entry(self, path):
        """
        Find a matching entry for path @path and return it. Returns
        None when no path matches. DIST entries are not included.
        """

        self.load_manifests_for_path(path)
        for mpath, relpath, m in self._iter_manifests_for_path(path):
            for e in m.entries:
                if e.tag == 'IGNORE':
                    # ignore matches recursively, so we process it separately
                    # py<3.5 does not have os.path.commonpath()
                    fullpath = os.path.join(relpath, e.path)
                    if gemato.util.path_starts_with(path, fullpath):
                        return e
                elif e.tag in ('DIST', 'TIMESTAMP'):
                    # distfiles are not local files, so skip them
                    # timestamp is not a file ;-)
                    pass
                else:
                    fullpath = os.path.join(relpath, e.path)
                    if fullpath == path:
                        return e
        return None

    def verify_path(self, relpath):
        """
        Verify the path @relpath against appropriate Manifest entry.
        If there is no matching entry, verification fails (as a stray
        file). Returns result as verify_path().
        """
        real_path = os.path.join(self.root_directory, relpath)
        path_entry = self.find_path_entry(relpath)
        return gemato.verify.verify_path(real_path, path_entry)

    def assert_path_verifies(self, relpath):
        """
        Verify the path @relpath against appropriate Manifest entry.
        If there is no matching entry, verification fails (as a stray
        file). Raises exception for failed verification.
        """
        real_path = os.path.join(self.root_directory, relpath)
        path_entry = self.find_path_entry(relpath)
        ret, diff = gemato.verify.verify_path(real_path, path_entry,
                expected_dev=self.manifest_device)
        if not ret:
            raise gemato.exceptions.ManifestMismatch(
                    relpath, path_entry, diff)

    def find_dist_entry(self, filename, relpath=''):
        """
        Find a matching entry for distfile @filename and return it.
        If @relpath is provided, loads all Manifests up to @relpath
        (which can be e.g. a relevant package directory).
        Returns None when no DIST entry matches.
        """

        self.load_manifests_for_path(relpath+'/')
        for mpath, p, m in self._iter_manifests_for_path(relpath+'/'):
            for e in m.entries:
                if e.tag == 'DIST' and e.path == filename:
                    return e
        return None

    def get_file_entry_dict(self, path='', only_types=None):
        """
        Find all file entries that apply to paths starting with @path.
        Return a dictionary mapping relative paths to entries. Raises
        an exception if multiple entries for file collide.

        If @only_types are specified as a list, only files of specified
        types will be collected. If it is not specified, then all types
        for local files will be processed.
        """

        self.load_manifests_for_path(path, recursive=True)
        out = {}
        for mpath, relpath, m in self._iter_manifests_for_path(path,
                                    recursive=True):
            for e in m.entries:
                if only_types is not None:
                    if e.tag not in only_types:
                        continue
                    # DIST entries always specify plain filename
                    if e.tag == 'DIST':
                        relpath = ''
                elif e.tag in ('DIST', 'TIMESTAMP'):
                    # distfiles are not local files, so skip them
                    # timestamp is not a file ;-)
                    continue

                fullpath = os.path.join(relpath, e.path)
                if gemato.util.path_starts_with(fullpath, path):
                    if fullpath in out:
                        # compare the two entries
                        ret, diff = gemato.verify.verify_entry_compatibility(
                                out[fullpath], e)
                        if not ret:
                            raise gemato.exceptions.ManifestIncompatibleEntry(out[fullpath], e, diff)
                        # we need to construct a single entry with both checksums
                        if diff:
                            new_checksums = dict(e.checksums)
                            for k, d1, d2 in diff:
                                if d2 is None:
                                    new_checksums[k] = d1
                            e = type(e)(e.path, e.size, new_checksums)
                    out[fullpath] = e
        return out

    def _verify_one_file(self, path, relpath, e, fail_handler, warn_handler):
        ret, diff = gemato.verify.verify_path(path, e,
                expected_dev=self.manifest_device)

        if not ret:
            if e is not None and e.tag in ('MISC', 'OPTIONAL'):
                h = warn_handler
            else:
                h = fail_handler
            err = gemato.exceptions.ManifestMismatch(relpath, e, diff)
            ret = h(err)
            if ret is None:
                ret = True

        return ret

    def assert_directory_verifies(self, path='',
            fail_handler=gemato.util.throw_exception,
            warn_handler=None):
        """
        Verify the complete directory tree starting at @path (relative
        to top Manifest directory). Includes testing for stray files.
        Raises an exception if any of the files does not pass
        verification.

        @fail_handler is the callback called whenever verification
        fails for 'strong' entries (or stray files). @warn_handler
        is called whenever verification fails for MISC/OPTIONAL entries.

        The handlers are passed a ManifestMismatch exception object.
        The default fail handler raises the exception. Unless specified
        explicitly, the warn handler defaults to fail handler. However,
        custom handlers can be used to provide a non-strict mode,
        or continue the scan after the first failure.

        If none of the handlers raise exceptions, the function returns
        boolean. It returns False if at least one of the handler calls
        returned explicit False; True otherwise.
        """

        manifest_filenames = (gemato.compression
                .get_potential_compressed_names('Manifest'))

        entry_dict = self.get_file_entry_dict(path)
        it = os.walk(os.path.join(self.root_directory, path),
                onerror=gemato.util.throw_exception,
                followlinks=True)
        ret = True

        if warn_handler is None:
            warn_handler = fail_handler

        for dirpath, dirnames, filenames in it:
            relpath = os.path.relpath(dirpath, self.root_directory)
            # strip dot to avoid matching problems
            if relpath == '.':
                relpath = ''

            skip_dirs = []
            for d in dirnames:
                # skip dotfiles
                if d.startswith('.'):
                    skip_dirs.append(d)
                    continue

                dpath = os.path.join(relpath, d)
                de = entry_dict.pop(dpath, None)
                if de is None:
                    syspath = os.path.join(dirpath, d)
                    st = os.stat(syspath)
                    if st.st_dev != self.manifest_device:
                        raise gemato.exceptions.ManifestCrossDevice(syspath)
                    continue

                if de.tag == 'IGNORE':
                    skip_dirs.append(d)
                else:
                    ret &= self._verify_one_file(os.path.join(dirpath, d),
                            dpath, de, fail_handler, warn_handler)

            # skip scanning ignored directories
            for d in skip_dirs:
                dirnames.remove(d)

            for f in filenames:
                # skip dotfiles
                if f.startswith('.'):
                    continue

                fpath = os.path.join(relpath, f)
                # skip top-level Manifest, we obviously can't have
                # an entry for it
                if fpath in manifest_filenames:
                    continue
                fe = entry_dict.pop(fpath, None)
                ret &= self._verify_one_file(os.path.join(dirpath, f),
                        fpath, fe, fail_handler, warn_handler)

        # check for missing files
        for relpath, e in entry_dict.items():
            syspath = os.path.join(self.root_directory, relpath)
            ret &= self._verify_one_file(syspath, relpath, e,
                            fail_handler, warn_handler)

        return ret

    def save_manifests(self, hashes=None, force=False, sort=False,
            compress_watermark=None, compress_format='gz'):
        """
        Save the Manifests modified since the last save_manifests()
        call.

        @hashes specifies the requested hash set. If specified,
        it overrides the hash set used in Manifest. If None, the set
        specified in ManifestLoader constructor is used. If that one
        is None as well, the routine reuses the existing hash set
        in the entry.

        If @force is True, all Manifests will be rewritten even
        if they were not modified.

        If @sort is True, the Manifest entries will be sorted prior
        to saving.

        If @compress_watermark is not None, then the uncompressed
        Manifest files whose size is larger than or equal to the value
        will be compressed using @compress_format. The Manifest files
        whose size is smaller will be uncompressed. To compress all
        Manifest files, pass a size of 0.
        
        If @compress_watermark is None, the compression is left as-is.
        """

        if hashes is None:
            hashes = self.hashes
        if force:
            self.load_manifests_for_path('', recursive=True)

        fixed_manifests = set()
        renamed_manifests = {}
        for mpath, relpath, m in self._iter_manifests_for_path('',
                                    recursive=True):
            for e in m.entries:
                if e.tag != 'MANIFEST':
                    continue

                if e.path in renamed_manifests:
                    e.path = renamed_manifests[e.path]
                fullpath = os.path.join(relpath, e.path)
                if not force and fullpath not in self.updated_manifests:
                    continue

                gemato.verify.update_entry_for_path(
                    os.path.join(self.root_directory, fullpath),
                    e,
                    hashes=hashes,
                    expected_dev=self.manifest_device)

                # do not remove it from self.updated_manifests
                # immediately as we may have to deal with multiple
                # entries
                fixed_manifests.add(fullpath)
                self.updated_manifests.add(mpath)

            # we've apparently modified this Manifest, so store it now
            if force or mpath in self.updated_manifests:
                unc_size = self.save_manifest(mpath, sort=sort)
                # let's see if we want to recompress it
                if compress_watermark is not None:
                    compr = (gemato.compression
                            .get_compressed_suffix_from_filename(mpath))
                    is_compr = compr is not None
                    is_large = unc_size >= compress_watermark
                    if is_compr != is_large:
                        if is_large:
                            # compress it!
                            new_mpath = mpath + '.' + compress_format
                        else:
                            new_mpath = mpath[:-len(compr)-1]

                        # do the rename!
                        self.loaded_manifests[new_mpath] = m
                        self.save_manifest(new_mpath)
                        del self.loaded_manifests[mpath]
                        os.unlink(os.path.join(self.root_directory,
                            mpath))
                        renamed_manifests[mpath] = new_mpath

        # now, discard all the Manifests whose entries we've updated
        self.updated_manifests -= fixed_manifests
        # ...and those which we renamed
        self.updated_manifests -= set(renamed_manifests.keys())
        # ...and top-level Manifest which has no entries
        self.updated_manifests -= set(gemato.compression
                .get_potential_compressed_names('Manifest'))
        # at this point, the list should be empty
        assert not self.updated_manifests, (
                "Unlinked but updated Manifests: {}".format(
                    self.updated_manifests))

    def update_entry_for_path(self, path, new_entry_type='DATA',
            hashes=None):
        """
        Update the Manifest entries for @path and queue the containing
        Manifests for update. @path must not be covered by IGNORE.
        You need to invoke save_manifests() to store the Manifest
        updates afterwards.

        If the path exists and has a matching Manifest entry, the most
        specific existing entry will be updated. If the path has more
        entries, the remaining entries will be removed. This function
        does not check if they were compatible.

        The type of MANIFEST, DATA and MISC derived entries
        is preserved. OPTIONAL entries are left as-is.

        If the path exists and has no Manifest entry, a new entry
        of type @new_entry_type will be created in the Manifest most
        specific to the location. Note that AUX entries can only
        be created if they're located in 'files/' directory relative
        to an existing Manifest.

        If the path does not exist, all Manifest entries for it will
        be removed except for OPTIONAL entries.

        @hashes specifies the requested hash set. If specified,
        it overrides the hash set used in Manifest. If None, the set
        specified in ManifestLoader constructor is used. If that one
        is None as well, the routine reuses the existing hash set
        in the entry.

        When creating a new entry, @hashes must be specified explicitly
        either via the function or on construction.
        """

        had_entry = False
        if hashes is None:
            hashes = self.hashes

        self.load_manifests_for_path(path)
        for mpath, relpath, m in self._iter_manifests_for_path(path):
            entries_to_remove = []
            for e in m.entries:
                if e.tag == 'IGNORE':
                    # ignore matches recursively, so we process it separately
                    # py<3.5 does not have os.path.commonpath()
                    fullpath = os.path.join(relpath, e.path)
                    assert not gemato.util.path_starts_with(path, fullpath)
                elif e.tag in ('DIST', 'TIMESTAMP'):
                    # distfiles are not local files, so skip them
                    # timestamp is not a file ;-)
                    pass
                elif e.tag == 'OPTIONAL':
                    # leave OPTIONAL entries as-is
                    fullpath = os.path.join(relpath, e.path)
                    if fullpath == path:
                        had_entry = True
                else:
                    # we update either file at the specified path
                    # or any relevant Manifests
                    fullpath = os.path.join(relpath, e.path)
                    if fullpath != path:
                        continue

                    if had_entry:
                        # duplicate entry!
                        entries_to_remove.append(e)
                        continue

                    try:
                        gemato.verify.update_entry_for_path(
                            os.path.join(self.root_directory,
                                fullpath),
                            e,
                            hashes=hashes,
                            expected_dev=self.manifest_device)
                    except gemato.exceptions.ManifestInvalidPath as err:
                        if err.detail[0] == '__exists__':
                            # file does not exist anymore, so remove
                            # the entry
                            entries_to_remove.append(e)
                            had_entry = True
                        else:
                            raise err
                    else:
                        self.updated_manifests.add(mpath)
                        had_entry = True

            if entries_to_remove:
                for e in entries_to_remove:
                    m.entries.remove(e)
                self.updated_manifests.add(mpath)

        if not had_entry:
            assert hashes is not None
            for mpath, relpath, m in self._iter_manifests_for_path(path):
                # add to the first relevant Manifest
                assert new_entry_type not in (
                        'DIST', 'IGNORE', 'OPTIONAL')
                newpath = os.path.relpath(path, relpath)
                if new_entry_type == 'AUX':
                    # AUX has implicit files/ prefix
                    assert gemato.util.path_inside_dir(newpath,
                            'files')
                    # drop files/ prefix
                    newpath = os.path.relpath(newpath, 'files')
                e = gemato.manifest.new_manifest_entry(
                        new_entry_type, newpath, 0, {})
                gemato.verify.update_entry_for_path(
                    os.path.join(self.root_directory, path),
                    e,
                    hashes=hashes,
                    expected_dev=self.manifest_device)
                m.entries.append(e)
                self.updated_manifests.add(mpath)
                had_entry = True
                break

    def get_deduplicated_file_entry_dict_for_update(self, path=''):
        """
        Find all file entries that apply to paths starting with @path.
        Remove all duplicate entries and queue the relevant Manifests
        for update. Return a dictionary mapping relative paths
        to tuple of (manifest path, entry).

        You need to invoke save_manifests() to store the Manifest
        updates afterwards. However, note that the resulting tree
        may no longer validate.

        If the path is referenced by multiple entries of incompatible
        semantics, raises an exception. If the entries have compatible
        semantics, all but the first (deepest) are removed, even
        if they have colliding sizes or hashes. If the duplicate
        entries use different hash sets, the preserved entry is updated
        to have the union of their hashes.
        """

        self.load_manifests_for_path(path, recursive=True)
        out = {}
        for mpath, relpath, m in self._iter_manifests_for_path(path,
                                    recursive=True):
            entries_to_remove = []
            for e in m.entries:
                if e.tag in ('DIST', 'TIMESTAMP'):
                    # distfiles are not local files, so skip them
                    # timestamp is not a file ;-)
                    continue

                fullpath = os.path.join(relpath, e.path)
                if gemato.util.path_starts_with(fullpath, path):
                    if fullpath in out:
                        # compare the two entries
                        ret, diff = gemato.verify.verify_entry_compatibility(
                                out[fullpath][1], e)
                        # if semantically incompatible, throw
                        if not ret and diff[0][0] == '__type__':
                            raise (gemato.exceptions
                                    .ManifestIncompatibleEntry(
                                        out[fullpath][1], e, diff))
                        # otherwise, make sure we have all checksums
                        out[fullpath][1].checksums.update(e.checksums)
                        # and drop the duplicate
                        entries_to_remove.append(e)
                    else:
                        out[fullpath] = (mpath, e)

            if entries_to_remove:
                for e in entries_to_remove:
                    m.entries.remove(e)
                self.updated_manifests.add(mpath)

        return out

    def load_unregistered_manifests(self, path=''):
        """
        Scan the directory @path (relative to top directory)
        for unregistered (not listed in MANIFEST entries) Manifest
        files and load them if they are valid.

        Returns a list of files found. The respective MANIFEST entries
        need to be added to other Manifests manually to ensure
        integrity. Note that the list may contain files that are
        referenced within added Manifests, so the list should
        be verified with regards to existing entries.
        """

        manifest_filenames = (gemato.compression
                .get_potential_compressed_names('Manifest'))

        entry_dict = self.get_file_entry_dict(path,
                only_types=['IGNORE'])
        new_manifests = []
        it = os.walk(os.path.join(self.root_directory, path),
                onerror=gemato.util.throw_exception,
                followlinks=True)

        for dirpath, dirnames, filenames in it:
            relpath = os.path.relpath(dirpath, self.root_directory)
            # strip dot to avoid matching problems
            if relpath == '.':
                relpath = ''

            skip_dirs = []
            for d in dirnames:
                # skip dotfiles
                if d.startswith('.'):
                    skip_dirs.append(d)
                    continue

                dpath = os.path.join(relpath, d)
                de = entry_dict.pop(dpath, None)
                if de is None:
                    syspath = os.path.join(dirpath, d)
                    st = os.stat(syspath)
                    if st.st_dev != self.manifest_device:
                        raise gemato.exceptions.ManifestCrossDevice(syspath)
                    continue

                assert de.tag == 'IGNORE'
                skip_dirs.append(d)

            # skip scanning ignored directories
            for d in skip_dirs:
                dirnames.remove(d)

            # check for unregistered Manifest
            for mname in manifest_filenames:
                if mname in filenames:
                    fpath = os.path.join(relpath, mname)
                    if fpath in self.loaded_manifests:
                        continue

                    # we've just found ourselves a new Manifest,
                    # let's try to load it
                    try:
                        self.load_manifest(fpath)
                    except gemato.exceptions.ManifestSyntaxError:
                        # syntax error? probably not a Manifest then.
                        pass
                    else:
                        new_manifests.append(fpath)

        return new_manifests


    def update_entries_for_directory(self, path='', hashes=None):
        """
        Update the Manifest entries for the contents of directory
        @path (top directory by default), recursively. Includes adding
        new files and removing entries for those that no longer exist.
        The behavior for various cases is the same
        as for update_entry_for_path() except as noted below.

        New entries are currently created with DATA type. This will
        be extended in the future.

        @hashes specifies the requested hash set. If specified,
        it overrides the hash set used in Manifest. If None, the set
        specified in ManifestLoader constructor is used. Either
        of the two hash sets must be specified.
        """

        if hashes is None:
            hashes = self.hashes
        assert hashes is not None

        manifest_filenames = (gemato.compression
                .get_potential_compressed_names('Manifest'))

        new_manifests = self.load_unregistered_manifests(path)
        entry_dict = self.get_deduplicated_file_entry_dict_for_update(
                path)
        manifest_stack = []
        for mpath, mrpath, m in (self
                ._iter_manifests_for_path(path)):
            manifest_stack.append((mpath, mrpath, m))
            break

        it = os.walk(os.path.join(self.root_directory, path),
                onerror=gemato.util.throw_exception,
                followlinks=True)

        for dirpath, dirnames, filenames in it:
            relpath = os.path.relpath(dirpath, self.root_directory)
            # strip dot to avoid matching problems
            if relpath == '.':
                relpath = ''

            # drop Manifest paths until we get to a common directory
            while not gemato.util.path_starts_with(relpath,
                    manifest_stack[-1][1]):
                manifest_stack.pop()

            want_manifest = self.profile.want_manifest_in_directory(
                    relpath, dirnames, filenames)

            skip_dirs = []
            for d in dirnames:
                # skip dotfiles
                if d.startswith('.'):
                    skip_dirs.append(d)
                    continue

                dpath = os.path.join(relpath, d)
                mpath, de = entry_dict.pop(dpath, (None, None))
                if de is None:
                    syspath = os.path.join(dirpath, d)
                    st = os.stat(syspath)
                    if st.st_dev != self.manifest_device:
                        raise gemato.exceptions.ManifestCrossDevice(syspath)
                    continue

                if de.tag == 'IGNORE':
                    skip_dirs.append(d)
                else:
                    # trigger the exception indirectly
                    gemato.verify.update_entry_for_path(
                        os.path.join(dirpath, d),
                        de,
                        hashes=hashes,
                        expected_dev=self.manifest_device)
                    assert False, "exception should have been raised"

            # skip scanning ignored directories
            for d in skip_dirs:
                dirnames.remove(d)

            new_entries = []
            for f in filenames:
                # skip dotfiles
                if f.startswith('.'):
                    continue

                fpath = os.path.join(relpath, f)
                mpath, fe = entry_dict.pop(fpath, (None, None))
                if fe is not None:
                    if fe.tag in ('IGNORE', 'OPTIONAL'):
                        continue
                    if fe.tag == 'MANIFEST':
                        manifest_stack.append((fpath, relpath,
                            self.loaded_manifests[fpath]))
                        # do not update the Manifest entry if
                        # the relevant Manifest is going to be updated
                        # anyway
                        if relpath in self.updated_manifests:
                            continue
                else:
                    # skip top-level Manifest, we obviously can't have
                    # an entry for it
                    if fpath in manifest_filenames:
                        continue
                    if fpath in new_manifests:
                        ftype = 'MANIFEST'
                        manifest_stack.append((fpath, relpath,
                            self.loaded_manifests[fpath]))
                    else:
                        ftype = self.profile.get_entry_type_for_path(
                                fpath)

                    # note: .path needs to be corrected below
                    fe = gemato.manifest.new_manifest_entry(ftype,
                            fpath, 0, {})
                    new_entries.append(fe)
                    if relpath in self.updated_manifests:
                        continue

                changed = gemato.verify.update_entry_for_path(
                    os.path.join(dirpath, f),
                    fe,
                    hashes=hashes,
                    expected_dev=self.manifest_device)
                if changed and mpath is not None:
                    self.updated_manifests.add(mpath)

            # do we have Manifest in this directory?
            if want_manifest and manifest_stack[-1][1] != relpath:
                mpath = os.path.join(relpath, 'Manifest')
                m = self.create_manifest(mpath)
                manifest_stack.append((mpath, relpath, m))
                fe = gemato.manifest.ManifestEntryMANIFEST(
                        mpath, 0, {})
                new_entries.append(fe)

            if new_entries:
                mpath, mdirpath, m = manifest_stack[-1]
                for fe in new_entries:
                    if fe.tag == 'MANIFEST':
                        # Manifest needs to go level up
                        mmpath = mpath
                        mm = m
                        mmdirpath = mdirpath
                        i = -1
                        while mmdirpath == os.path.dirname(fe.path):
                            i -= 1
                            mmpath, mmdirpath, mm = manifest_stack[i]

                        fe.path = os.path.relpath(fe.path, mmdirpath)
                        mm.entries.append(fe)
                        self.updated_manifests.add(mmpath)
                    else:
                        if ftype == 'AUX':
                            # AUX has implicit files/ prefix in .path
                            # but for now, we've shoved our path
                            # into .aux_path
                            fe.path = os.path.relpath(fe.aux_path,
                                    mdirpath)
                            assert gemato.util.path_inside_dir(
                                    fe.path, 'files')
                            # drop files/ prefix for the entry
                            fe.aux_path = os.path.relpath(fe.path,
                                    'files')
                        else:
                            fe.path = os.path.relpath(fe.path, mdirpath)
                        m.entries.append(fe)
                self.updated_manifests.add(mpath)

        # check for removed files
        for relpath, me in entry_dict.items():
            mpath, fe = me
            if fe.tag in ('IGNORE', 'OPTIONAL'):
                continue

            self.loaded_manifests[mpath].entries.remove(fe)
            self.updated_manifests.add(mpath)

    def create_manifest(self, path):
        """
        Create a new empty sub-Manifest instance at relative path @path.
        The file will not be written until save_manifests(). No MANIFEST
        entry for the file will be created.

        Returns the new ManifestFile instance.
        """

        return self.load_manifest(path, allow_create=True)
