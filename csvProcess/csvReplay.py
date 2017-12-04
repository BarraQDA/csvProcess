#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Jonathan Schultz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
import gooey
import argparse
import os
import sys
import re
from datetime import datetime
import subprocess

# Work out whether the user wants gooey before gooey has a chance to strip the argument
gui = '--gui' in sys.argv

def add_arguments(parser):
    parser.description = "Replay twitter file processing"

    replaygroup = parser.add_argument_group('Replay')
    replaygroup.add_argument('input_file', type=str, widget='FileChooser',
                             help='File to replay.')
    replaygroup.add_argument('-f', '--force',   action='store_true',
                             help='Replay even if input file is not older than its dependents.')
    replaygroup.add_argument(      '--dry-run', action='store_true',
                             help='Print but do not execute command')
    replaygroup.add_argument(      '--edit', action='store_false',
                             help='Open a Gooey window to allow editing before running command')

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity', type=int, default=1)
    advancedgroup.add_argument('-d', '--depth',     type=int, default=1,
                               help='Depth of command history to replay.')
    advancedgroup.add_argument('-r', '--remove',   action='store_true',
                               help='Remove input file before replaying.')

    parser.set_defaults(func=csvReplay)
    parser.set_defaults(build_comments=build_comments)

@gooey.Gooey(ignore_command=None, force_command='--gui',
             default_cols=1,
             load_cmd_args=True, use_argparse_groups=True, use_tabs=True)
def parse_arguments():
    parser = gooey.GooeyParser()
    add_arguments(parser)
    args = parser.parse_args()
    return vars(args)

# Need to replicate this because parse_known_args doesn't work with gooey
def parse_arguments_no_gooey():
    parser = argparse.ArgumentParser()

    parser.description = "Replay twitter file processing"

    replaygroup = parser.add_argument_group('Replay')
    replaygroup.add_argument('input_file', type=str, nargs='+',
                             help='CSV files to replay.')
    replaygroup.add_argument('-f', '--force',   action='store_true',
                             help='Replay even if infile is not older than its dependents.')
    replaygroup.add_argument(      '--dry-run', action='store_true',
                             help='Print but do not execute command')
    replaygroup.add_argument(      '--edit', action='store_true',
                             help='Open a Gooey window to allow editing before running command')

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity', type=int, default=1)
    advancedgroup.add_argument('-d', '--depth',     type=int, default=1,
                               help='Depth of command history to replay.')
    advancedgroup.add_argument('-r', '--remove',   action='store_true',
                               help='Remove input file before replaying.')

    parser.set_defaults(func=csvReplay)
    parser.set_defaults(build_comments=build_comments)

    args, extraargs = parser.parse_known_args()
    args.extraargs = extraargs
    return vars(args)

def build_comments(kwargs):
    return ''

def csvReplay(input_file, force, dry_run, edit,
              verbosity, depth, remove,
              extraargs=[], **dummy):
    fileregexp = re.compile(r"^#+ (?P<file>.+) #+$", re.UNICODE)
    cmdregexp  = re.compile(r"^#\s+(?P<cmd>[\w\.-]+)", re.UNICODE)
    argregexp  = re.compile(r"^#\s+(?:--)?(?P<name>[\w-]+)(?:=(?P<quote>\"?)(?P<value>.+)(?P=quote))?", re.UNICODE)
    piperegexp = re.compile(r"^#+$", re.UNICODE)

    if not isinstance(input_file, list):    # Gooey can't handle input_file as list
        input_file = [input_file]

    for infilename in input_file:
        if verbosity >= 1:
            print("Replaying " + infilename, file=sys.stderr)

        # Read comments at start of infile.
        infile = file(infilename, 'rU')
        comments = []
        while True:
            line = infile.readline()
            if line[:1] == '#':
                comments.append(line)
            else:
                infile.close()
                break

        if remove:
            os.remove(infilename)

        curdepth = 0
        replaystack = []
        commentline = comments.pop(0)
        filematch = fileregexp.match(commentline)
        while filematch:
            filename  = filematch.group('file')
            pipestack = []
            infilelist = []
            outfile = None
            pipematch = True
            while pipematch and len(comments) > 0:
                commentline = comments.pop(0)
                cmdmatch = cmdregexp.match(commentline)
                if cmdmatch:
                    cmd = cmdmatch.group('cmd')
                else:
                    break

                arglist = []
                lastargname = ''
                commentline = comments.pop(0) if len(comments) > 0 else None
                argmatch = argregexp.match(commentline) if commentline else None
                while argmatch:
                    argname  = argmatch.group('name')
                    argvalue = argmatch.group('value')

                    if argname == 'infile':
                        if argvalue != '<stdin>':
                            infilelist.append(argvalue)
                    else:
                        if argname == 'outfile':
                            if argvalue != '<stdout>':
                                outfile = argvalue
                                assert (outfile == filename)
                        else:
                            if argname != lastargname:
                                arglist.append('--' + argname)
                                lastargname = argname

                            if argvalue is not None:
                                arglist.append(argvalue)

                    commentline = comments.pop(0) if len(comments) > 0 else None
                    argmatch = argregexp.match(commentline) if commentline else None

                pipestack.append((cmd, arglist + extraargs + ['--verbosity', str(verbosity)]))
                pipematch = piperegexp.match(commentline) if commentline else None

            if outfile:
                replaystack.append((pipestack, infilelist, outfile))

            curdepth += 1
            if curdepth == depth:
                break

            filematch = fileregexp.match(commentline) if commentline else None


        if replaystack:
            (pipestack, infilelist, outfilename) = replaystack.pop()
        else:
            pipestack = None

        execute = force
        while pipestack:
            if not execute:
                outfilestamp = (datetime.utcfromtimestamp(os.path.getmtime(outfilename)) if os.path.isfile(outfilename) else None) if outfilename else None
                if not infilelist:
                    execute = True
                for infilename in infilelist:
                    infilestamp = (datetime.utcfromtimestamp(os.path.getmtime(infilename)) if os.path.isfile(infilename) else None) if infilename else None
                    if infilestamp > (outfilestamp or datetime.min):
                        execute = True
                        break

            if execute:
                process = None
                while len(pipestack) > 0:
                    (cmd, arglist) = pipestack.pop()
                    if infilelist:
                        arglist = infilelist + arglist
                    infilelist = None
                    if len(pipestack) == 0 and '--outfile' not in arglist:
                        arglist = arglist + ['--outfile', outfilename]

                    if edit:
                        arglist += ['--gui']

                    if verbosity >= 1:
                        print("Executing: " + cmd + ' ' + ' '.join(arglist), file=sys.stderr)

                    if not dry_run:
                        if not process:
                            process = subprocess.Popen([cmd] + arglist,
                                                       stdout=subprocess.PIPE if len(pipestack) else sys.stdout,
                                                       stderr=sys.stderr)
                        else:
                            process = subprocess.Popen([cmd] + arglist,
                                                       stdout=subprocess.PIPE if len(pipestack) else sys.stdout,
                                                       stdin=process.stdout,
                                                       stderr=sys.stderr)
                if not dry_run:
                    process.wait()
            else:
                if verbosity >= 2:
                    print("File not replayed: " + outfilename, file=sys.stderr)

            if replaystack:
                (pipestack, infilelist, outfilename) = replaystack.pop()
            else:
                pipestack = None

def main():
    if gui:
        kwargs = parse_arguments()
        kwargs['func'](**kwargs)
    else:
        kwargs = parse_arguments_no_gooey()
        kwargs['func'](**kwargs)

if __name__ == '__main__':
    main()
