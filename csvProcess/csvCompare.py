#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Jonathan Schultz
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
import argparse
import sys
import os
import shutil
import unicodecsv
import re

def csvCompare(arglist):
    parser = argparse.ArgumentParser(description='Compare two CSV files.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
    parser.add_argument('-b', '--batch',     type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',   type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

    parser.add_argument('-c', '--column',    type=str, default='word', help='Text column')
    parser.add_argument('-s', '--score',     type=str, help='Python expression for score')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')

    parser.add_argument('infile1', type=str, help='Input CSV files to compare.')
    parser.add_argument('infile2', type=str, help='Input CSV files to compare.')

    parser.add_argument('--no-comments',     action='store_true',
                                              help='Do not produce a comments logfile')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    args = parser.parse_args(arglist)
    hiddenargs = ['verbosity', 'jobs', 'batch', 'no_comments']

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        exec(os.linesep.join(args.prelude), globals())

    infile1 = open(args.infile1, 'rU')
    infile2 = open(args.infile2, 'rU')

    # Skip comments at start of infiles.
    in1comments = ''
    while True:
        line = infile1.readline()
        if line[:1] == '#':
            in1comments += line
        else:
            in1fieldnames = next(unicodecsv.reader([line]))
            break
    in2comments = ''
    while True:
        line = infile2.readline()
        if line[:1] == '#':
            in2comments += line
        else:
            in2fieldnames = next(unicodecsv.reader([line]))
            break

    in1reader=unicodecsv.DictReader(infile1, fieldnames=in1fieldnames)
    in2reader=unicodecsv.DictReader(infile2, fieldnames=in2fieldnames)

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = open(args.outfile, 'w')

    if not args.no_comments:
        if args.outfile:
            comments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            comments = '#' * 80 + '\n'
        comments += '# ' + os.path.basename(sys.argv[0]) + '\n'
        arglist = args.__dict__.keys()
        for arg in arglist:
            if arg not in hiddenargs:
                val = getattr(args, arg)
                if type(val) == str or type(val) == unicode:
                    comments += '#     --' + arg + '="' + val + '"\n'
                elif type(val) == bool:
                    if val:
                        comments += '#     --' + arg + '\n'
                elif type(val) == list:
                    for valitem in val:
                        if type(valitem) == str:
                            comments += '#     --' + arg + '="' + valitem + '"\n'
                        else:
                            comments += '#     --' + arg + '=' + str(valitem) + '\n'
                elif val is not None:
                    comments += '#     --' + arg + '=' + str(val) + '\n'

        outfile.write(comments.encode('utf8'))
        outfile.write(in1comments)
        outfile.write(in2comments)

    def clean(v):
        return re.sub('\W|^(?=\d)','_', v)

    exec("\
def evalscore1(" + ','.join([clean(fieldname) for fieldname in in1fieldnames]) + ",**kwargs):\n\
    return " + args.score, globals())

    exec("\
def evalscore2(" + ','.join([clean(fieldname) for fieldname in in2fieldnames]) + ",**kwargs):\n\
    return " + args.score, globals())

    dict1 = {}
    index = 0
    for row in in1reader:
        rowargs = {clean(key): value for key, value in row.iteritems()}
        dict1[row[args.column]] = evalscore1(**rowargs)
        index += 1
    dict2 = {}
    index = 0
    for row in in2reader:
        rowargs = {clean(key): value for key, value in row.iteritems()}
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

    outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=[args.column, args.score],
                                        extrasaction='ignore', lineterminator=os.linesep)
    if not args.no_header:
        outunicodecsv.writeheader()
    for diff in sorteddiff:
        outunicodecsv.writerow({args.column: diff[0], args.score: diff[1]})

    outfile.close()


if __name__ == '__main__':
    csvCompare(None)
