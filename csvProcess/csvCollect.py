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
import string
import multiprocessing
import pymp
import re
from dateutil import parser as dateparser
import calendar
from pytimeparse.timeparse import timeparse
from operator import sub, add
import subprocess
import datetime
from decimal import *
import itertools
from more_itertools import peekable

def csvCollect(arglist=None):

    parser = ArgumentRecorder(description='CSV data collection.',
                              fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1, private=True)
    parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.', private=True)
    parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of rows to process per batch. Use to limit memory usage with very large files. May affect performance but not results.', private=True)

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether row is included')
    parser.add_argument(      '--since',      type=str, help='Lower bound date/time in any sensible format.')
    parser.add_argument(      '--until',      type=str, help='Upper bound date/time in any sensible format.')
    parser.add_argument(      '--datecol',    type=str, help='Column containing date/time date', default='date')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of rows to process')

    parser.add_argument('-r', '--regexp',     type=str, help='Regular expression to create values to collect.')
    parser.add_argument('-c', '--column',     type=str, help='Column to apply regular expression, default is "text"')
    parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
    parser.add_argument('-I', '--indexes',    type=str, nargs="*", help='Python code to produce lists of values to collect.')
    parser.add_argument('-H', '--header',     type=str, nargs="*", help='Column name for regexp or indexes result.')

    parser.add_argument('-sh', '--score-header', type=str, nargs="*", help='Names of columns to create for row scores.')
    parser.add_argument('-s', '--score',      type=str, nargs="*", default=['1'], help='Python expression(s) to evaluate row score(s), for example "1 + retweets + favorites"')
    parser.add_argument('-t', '--threshold',  type=float, help='Threshold (first) score for result to be output')

    parser.add_argument('-in', '--interval',  type=str, help='Interval for measuring frequency, for example "1 day".')

    parser.add_argument('-S', '--sort',       type=str, nargs="?", help='Python expression used to sort rows.')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.', output=True)
    parser.add_argument('-n', '--number',     type=int, help='Maximum number of results to output')
    parser.add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('-P', '--pipe', type=str,            help='Command to pipe input from')
    parser.add_argument('infile',       type=str, nargs='?', help='Input CSV file, if neither input nor pipe is specified, stdin is used.', input=True)

    args = parser.parse_args(arglist)

    if (args.regexp is None) == (args.indexes is None):
        raise RuntimeError("Exactly one of 'indexes' and 'regexp' must be specified.")

    if args.regexp and not args.column:
        raise RuntimeError("'column' must be specified for regexp.")

    if args.interval:   # Multiprocessing requires single thread
        args.jobs = 1
    else:
        if args.jobs is None:
            args.jobs = multiprocessing.cpu_count()

        if args.verbosity >= 1:
            print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

        if args.batch is None:
            args.batch = sys.maxint

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)
        if args.verbosity >= 2:
            print(os.linesep.join(args.prelude), file=sys.stderr)

        exec(os.linesep.join(args.prelude), globals())

    fields = []
    if args.regexp:
        regexp = re.compile(args.regexp, re.IGNORECASE if args.ignorecase else 0)
        fields += list(regexp.groupindex)

    if args.indexes:
        if args.header:
            fields += args.header
            if len(args.indexes) != len(fields):
                raise RuntimeError("Number of column headers must equal number of data items.")
        else:
            fields = list(range(1, len(args.indexes)+1))

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    if args.interval:
        interval = timeparse(args.interval)
        if interval is None:
            raise RuntimeError("Interval: " + args.interval + " not recognised.")
        if args.verbosity >= 2:
            print("Interval is " + str(interval), file=sys.stderr)

    if args.infile:
        infile = open(args.infile, 'r')
    elif args.pipe:
        infile = peekable(subprocess.Popen(args.pipe, stdout=subprocess.PIPE, shell=True, text=True).stdout)
    else:
        infile = peekable(sys.stdin)

    # Read comments at start of infile.
    incomments = ArgumentHelper.read_comments(infile) or ArgumentHelper.separator()
    infieldnames = next(csv.reader([next(infile)]))
    inreader=csv.DictReader(infile, fieldnames=infieldnames)

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = open(args.outfile, 'w')

    if not args.no_comments:
        outfile.write(parser.build_comments(args, args.outfile) + incomments)

    # Dynamic code for filter, data and score
    def clean(v):
        return re.sub(r"\W|^(?=\d)",'_', str(v))

    if args.filter:
        if args.verbosity >= 2:
            print("\
def evalfilter(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return " + args.filter, file=sys.stderr)
        exec("\
def evalfilter(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return " + args.filter, globals())

    if args.indexes:
        if args.verbosity >= 2:
            print("\
def evalindexes(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return (list(itertools.zip_longest(*[" + ','.join(args.indexes) + "])))", file=sys.stderr)
        exec("\
def evalindexes(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return (list(itertools.zip_longest(*[" + ','.join(args.indexes) + "])))", globals())

    if args.score_header is None:
        if args.score == ['1']:
            args.score_header = ['frequency']
        else:
            args.score_header = []

    args.score_header = [args.score_header[scoreidx] or args.score[scoreidx]
                            if scoreidx < len(args.score_header)
                            else args.score[scoreidx] for scoreidx in range(len(args.score))]

    if args.sort:
        if args.verbosity >= 2:
            print("\
def evalsort(" + ','.join([clean(fieldname) for fieldname in fields+args.score_header]) + ",**kwargs):\n\
    return (" + args.sort + ")", file=sys.stderr)
        exec("\
def evalsort(" + ','.join([clean(fieldname) for fieldname in fields+args.score_header]) + ",**kwargs):\n\
    return (" + args.sort + ")", globals())

        def sortkey(row):
            rowargs = {clean(key): value for key, value in row.items()}
            return evalsort(**rowargs)

    if args.verbosity >= 2:
        print("\
def evalscore(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return [" + ','.join(args.score) + "]", file=sys.stderr)
    exec("\
def evalscore(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return [" + ','.join(args.score) + "]", globals())

    if args.verbosity >= 1:
        print("Loading CSV data.", file=sys.stderr)

    inrowcount = 0
    # NB Code for single- and multi-threaded processing is separate
    mergedresult = {}
    if args.jobs == 1:
        rows=[]
        if args.interval:
            runningresult = {}
        while True:
            if args.limit and inrowcount == args.limit:
                break

            try:
                while True:
                    row = next(inreader)
                    inrowcount += 1
                    rowargs = {clean(key): value for key, value in row.items()}
                    keep = True
                    if args.filter:
                        if args.verbosity >= 2:
                            print("evalfilter(" + repr(rowargs) + ")", file=sys.stderr)
                        keep = evalfilter(**rowargs) or False
                        if args.verbosity >= 2:
                            print("    --> " + repr(keep), file=sys.stderr)
                    if keep and (since or until):
                        date = row.get(args.datecol)
                        if date:
                            date = dateparser.parse(date)
                            if until and date >= until:
                                keep = False
                            elif since and date < since:
                                keep = False

                    if keep:
                        break

            except StopIteration:
                break

            # Deal with frequency calculation using column args.datecol
            if args.interval:
                row['datesecs'] = calendar.timegm(dateparser.parse(row[args.datecol]).timetuple())
                firstrow = rows[0] if len(rows) else None
                while firstrow and firstrow['datesecs'] - row['datesecs'] > interval:
                    indexes  = firstrow['indexes']
                    rowscore = firstrow['score']
                    for index in indexes:
                        runningresult[index] = list(map(sub, runningresult[index], rowscore))

                    del rows[0]
                    firstrow = rows[0] if len(rows) else None

            rowscore = None
            indexes = []
            if args.regexp:
                matches = regexp.finditer(row[args.column])

                for match in matches:
                    if not rowscore:
                        if args.verbosity >= 2:
                            print("evalscore(" + repr(rowargs) + ")", file=sys.stderr)
                        rowscore = evalscore(**rowargs)
                        if args.verbosity >= 2:
                            print("    --> " + repr(rowscore), file=sys.stderr)

                    if args.ignorecase:
                        index = tuple(value.lower() for value in match.groupdict().values())
                    else:
                        index = tuple(match.groupdict().values())

                    if args.interval:
                        if args.verbosity >= 2:
                            print("index = " + repr(index), file=sys.stderr)
                        indexes.append(index)
                        runningresult[index] = list(map(add, runningresult.get(index, [0] * len(args.score)), rowscore))
                        curmergedresult = mergedresult.get(index, [0] * len(args.score))
                        mergedresult[index] = [max(curmergedresult[idx], runningresult[index][idx]) for idx in range(len(args.score))]
                    else:
                        mergedresult[index] = list(map(add, mergedresult.get(index, [0] * len(args.score)), rowscore))

            if args.indexes:
                if args.verbosity >= 2:
                    print("evalindexes(" + repr(rowargs) + ")", file=sys.stderr)
                matches = evalindexes(**rowargs)
                if args.verbosity >= 2:
                    print("    --> " + repr(matches), file=sys.stderr)
                if args.verbosity >= 1:
                    if type(matches) != list:
                        print("WARNING: evalindexes should return a list, your 'indexes' argument is probably incorrect!", file=sys.stderr)

                for match in matches:
                    if not rowscore:
                        if args.verbosity >= 2:
                            print("evalscore(" + repr(rowargs) + ")", file=sys.stderr)
                        rowscore = evalscore(**rowargs)
                        if args.verbosity >= 2:
                            print("    --> " + repr(rowscore), file=sys.stderr)

                    if args.ignorecase:
                        index = match.lower()
                    else:
                        index = match

                    if args.interval:
                        if args.verbosity >= 2:
                            print("index = " + repr(index), file=sys.stderr)
                        indexes.append(index)
                        runningresult[index] = list(map(add, runningresult.get(index, [0] * len(args.score)), rowscore))
                        curmergedresult = mergedresult.get(index, [0] * len(args.score))
                        mergedresult[index] = [max(curmergedresult[idx], runningresult[index][idx]) for idx in range(len(args.score))]
                    else:
                        mergedresult[index] = list(map(add, mergedresult.get(index, [0] * len(args.score)), rowscore))

            if args.interval and rowscore:
                row['score']   = rowscore
                row['indexes'] = indexes
                rows.append(row)

    else:
        while True:
            if args.verbosity >= 2:
                print("Loading CSV batch.", file=sys.stderr)

            rows = []
            batchcount = 0
            while batchcount < args.batch:
                if args.limit and inrowcount == args.limit:
                    break
                try:
                    rows.append(next(inreader))
                    inrowcount += 1
                    batchcount += 1
                except StopIteration:
                    break

            if batchcount == 0:
                break

            if args.verbosity >= 2:
                print("Processing CSV batch.", file=sys.stderr)

            rowcount = len(rows)
            results = pymp.shared.list()
            with pymp.Parallel(args.jobs) as p:
                result = {}
                for rowindex in p.range(0, rowcount):
                    row = rows[rowindex]
                    rowargs = {clean(key): value for key, value in row.items()}
                    keep = True
                    if args.filter:
                        if args.verbosity >= 2:
                            print("evalfilter(" + repr(rowargs) + ")", file=sys.stderr)
                        keep = evalfilter(**rowargs) or False
                        if args.verbosity >= 2:
                            print("    --> " + repr(keep), file=sys.stderr)
                    if keep and (since or until):
                        date = row.get(args.datecol)
                        if date:
                            date = dateparser.parse(date)
                            if until and date >= until:
                                keep = False
                            elif since and date < since:
                                keep = False

                    if not keep:
                        continue

                    rowscore = None
                    if args.regexp:
                        matches = regexp.finditer(row[args.column])
                        rowscore = None
                        for match in matches:
                            if not rowscore:
                                if args.verbosity >= 2:
                                    print("evalscore(" + repr(rowargs) + ")", file=sys.stderr)
                                rowscore = evalscore(**rowargs)
                                if args.verbosity >= 2:
                                    print("    --> " + repr(rowscore), file=sys.stderr)

                            if args.ignorecase:
                                index = tuple(value.lower() for value in match.groupdict().values())
                            else:
                                index = tuple(match.groupdict().values())

                            result[index] = list(map(add, result.get(index, [0] * len(args.score)), rowscore))

                    if args.indexes:
                        if args.verbosity >= 2:
                            print("evalindexes(" + repr(rowargs) + ")", file=sys.stderr)
                        matches = evalindexes(**rowargs)
                        if args.verbosity >= 2:
                            print("    --> " + repr(matches), file=sys.stderr)
                        if args.verbosity >= 1:
                            if type(matches) != list:
                                print("WARNING: evalindexes should return a list, your 'indexes' argument is probably incorrect!", file=sys.stderr)

                        for match in matches:
                            if not rowscore:
                                if args.verbosity >= 2:
                                    print("evalscore(" + repr(rowargs) + ")", file=sys.stderr)
                                rowscore = evalscore(**rowargs)
                                if args.verbosity >= 2:
                                    print("    --> " + repr(rowscore), file=sys.stderr)

                            if args.ignorecase:
                                index = match.lower()
                            else:
                                index = match

                            result[index] = list(map(add, result.get(index, [0] * len(args.score)), rowscore))

                if args.verbosity >= 2:
                    print("Thread " + str(p.thread_num) + " found " + str(len(result)) + " results.", file=sys.stderr)

                with p.lock:
                    results.append(result)

            for result in results:
                for index in result:
                    mergedresult[index] = list(map(add, mergedresult.get(index, [0] * len(args.score)), result[index]))

    if args.verbosity >= 1:
        print("Sorting " + str(len(mergedresult)) + " results.", file=sys.stderr)
    if args.verbosity >= 2:
        print("    --> " +repr(mergedresult), file=sys.stderr)

    if args.sort:
        results = []
        for match in mergedresult.keys():
            if mergedresult[match][0] >= (args.threshold or 0):
                result = {}
                for idx in range(len(fields)):
                    result[fields[idx]] = match[idx]
                for idx in range(len(args.score)):
                    result[args.score_header[idx]] = mergedresult[match][idx]

            results.append(result)

        sortedresult = sorted(results, key=sortkey)
        if args.number:
            sortedresult = sortedresult[0:args.number]
    else:
        # Sort on first score value
        sortedresult = sorted([{'match': match, 'score':mergedresult[match]}
                                for match in mergedresult.keys() if mergedresult[match][0] >= (args.threshold or 0)],
                                key=lambda item: (-item['score'][0], item['match']))

        if args.number:
            sortedresult = sortedresult[0:args.number]

        for result in sortedresult:
            for idx in range(len(fields)):
                result[fields[idx]] = result['match'][idx]
            for idx in range(len(args.score)):
                result[args.score_header[idx]] = result['score'][idx]

    outcsv=csv.DictWriter(outfile, fieldnames=fields + args.score_header,
                          extrasaction='ignore', lineterminator=os.linesep)
    if not args.no_header:
        outcsv.writeheader()
    if len(sortedresult) > 0:
        outcsv.writerows(sortedresult)
    outfile.close()

if __name__ == '__main__':
    csvCollect(None)
