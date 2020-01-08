#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2019 Jonathan Schultz
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

from argrecord import ArgumentHelper, ArgumentRecorder
import sys
import os
import shutil
import csv
import re

def csvCompare(arglist):
    parser = ArgumentRecorder(description='Compare two CSV files.',
                              fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1, private=True)
    parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
    parser.add_argument('-b', '--batch',     type=int, default=100000, help='Number of rows to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',   type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-l', '--limit',     type=int, help='Limit number of rows to process')

    parser.add_argument('-c', '--column',    type=str, default='word', help='Text column')
    parser.add_argument('-s', '--score',     type=str, help='Python expression for score')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')

    parser.add_argument('infile1', type=str, help='Input CSV files to compare.', input=True)
    parser.add_argument('infile2', type=str, help='Input CSV files to compare.', input=True)

    parser.add_argument('--no-comments',     action='store_true',
                                              help='Do not produce a comments logfile')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    args = parser.parse_args(arglist)

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        exec(os.linesep.join(args.prelude), globals())

    infile1 = open(args.infile1, 'r')
    infile2 = open(args.infile2, 'r')

    # Read comments at start of infiles.
    incomments1 = ArgumentHelper.read_comments(infile1)
    infieldnames1 = next(csv.reader([next(infile1)]))
    inreader1=csv.DictReader(infile1, fieldnames=infieldnames1)

    incomments2 = ArgumentHelper.read_comments(infile2)
    infieldnames2 = next(csv.reader([next(infile2)]))
    inreader2=csv.DictReader(infile2, fieldnames=infieldnames2)

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = open(args.outfile, 'w')

    if not args.no_comments:
        outfile.write(parser.build_comments(args, args.outfile) + ((incomments1 + incomments2) or ArgumentHelper.separator()))

    def clean(v):
        return re.sub('\W|^(?=\d)','_', v)

    exec("\
def evalscore1(" + ','.join([clean(fieldname) for fieldname in infieldnames1]) + ",**kwargs):\n\
    return " + args.score, globals())

    exec("\
def evalscore2(" + ','.join([clean(fieldname) for fieldname in infieldnames2]) + ",**kwargs):\n\
    return " + args.score, globals())

    dict1 = {}
    index = 0
    for row in inreader1:
        rowargs = {clean(key): value for key, value in row.items()}
        dict1[row[args.column]] = evalscore1(**rowargs)
        index += 1
    dict2 = {}
    index = 0
    for row in inreader2:
        rowargs = {clean(key): value for key, value in row.items()}
        dict2[row[args.column]] = evalscore2(**rowargs)
        index += 1

    diff = {}
    for key, score1 in dict1.items():
        score2 = dict2.get(key, None)
        if score2:
            diff[key] = score2 - score1
        else:
            diff[key] = - score1

    for key, score2 in dict2.items():
        score1 = dict1.get(key, None)
        if not score1:
            diff[key] = score2

    sorteddiff = sorted([(key, delta) for key, delta in diff.items()],
                        key=lambda item: item[1])

    outcsv=csv.DictWriter(outfile, fieldnames=[args.column, args.score],
                          extrasaction='ignore', lineterminator=os.linesep)
    if not args.no_header:
        outcsv.writeheader()
    for diff in sorteddiff:
        outcsv.writerow({args.column: diff[0], args.score: diff[1]})

    outfile.close()


if __name__ == '__main__':
    csvCompare(None)
